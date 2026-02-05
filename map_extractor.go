package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"strings"

	"google.golang.org/protobuf/proto"

	// Importar a biblioteca demoinfocs
	"github.com/markus-wa/demoinfocs-golang/v5/pkg/demoinfocs/msg"
)

// MapExtractor extrai o nome do mapa de fragmentos de broadcast GOTV+
type MapExtractor struct {
	knownMaps map[string]bool
}

// NewMapExtractor cria um novo extrator de mapa
func NewMapExtractor() *MapExtractor {
	return &MapExtractor{
		knownMaps: map[string]bool{
			// Mapas competitivos CS2
			"de_dust2":    true,
			"de_mirage":   true,
			"de_inferno":  true,
			"de_nuke":     true,
			"de_vertigo":  true,
			"de_ancient":  true,
			"de_anubis":   true,
			"de_overpass": true,
			"de_train":    true,
			"de_cache":    true,
			"de_cbble":    true,
			"de_season":   true,
			"de_tuscan":   true,
			// Mapas de refém
			"cs_office": true,
			"cs_italy":  true,
			// Mapas curtos/workshop
			"de_shortnuke":     true,
			"de_shortdust":     true,
			"de_shortinferno":  true,
			"de_shortmirage":   true,
			"de_shortoverpass": true,
			"de_shortcache":    true,
			"de_shorttrains":   true,
			"de_shortnuke2":    true,
			// Mapas de armas
			"ar_shoots":   true,
			"ar_baggage":  true,
			"ar_monastery": true,
			// Mapas deathmatch
			"dm_workshop": true,
			// Novos mapas CS2
			"de_mills":   true,
			"de_basalt":  true,
			"de_edin":    true,
			"de_thera":   true,
		},
	}
}

// ExtractMapFromFragments tenta extrair o nome do mapa de um slice de fragmentos
// Funciona tentando parsear o CDemoFileHeader que geralmente vem no primeiro fragmento
func (me *MapExtractor) ExtractMapFromFragments(fragments [][]byte) (string, error) {
	// Tentar parsear cada fragmento como CDemoFileHeader
	for i, fragment := range fragments {
		if len(fragment) == 0 {
			continue
		}

		// Tentar parsear como protobuf CDemoFileHeader
		// O CDemoFileHeader é geralmente o primeiro message no demo
		mapName, err := me.extractFromProtobuf(fragment)
		if err == nil && mapName != "" {
			log.Printf("[MapExtractor] Mapa encontrado no fragmento %d via protobuf: %s", i, mapName)
			return mapName, nil
		}

		// Se não conseguir via protobuf, tentar buscar strings ASCII conhecidas
		mapName = me.extractFromASCII(fragment)
		if mapName != "" {
			log.Printf("[MapExtractor] Mapa encontrado no fragmento %d via ASCII: %s", i, mapName)
			return mapName, nil
		}
	}

	return "", fmt.Errorf("nenhum mapa encontrado nos fragmentos")
}

// extractFromProtobuf tenta extrair o mapa parsando o fragmento como protobuf
func (me *MapExtractor) extractFromProtobuf(data []byte) (string, error) {
	// Tentar parsear como CDemoFileHeader
	header := &msg.CDemoFileHeader{}
	err := proto.Unmarshal(data, header)
	if err == nil && header.GetMapName() != "" {
		return header.GetMapName(), nil
	}

	// Se falhar, tentar encontrar o CDemoFileHeader dentro do fragmento
	// Os fragmentos podem conter múltiplas mensagens
	// Procurar por padrões de protobuf que indicam um string field (field 5 = map_name)
	// Field 5 em protobuf é codificado como: 0x2A (field 5, wire type 2 = length-delimited)
	mapName := me.extractMapNameFromProtobufBytes(data)
	if mapName != "" {
		return mapName, nil
	}

	return "", fmt.Errorf("CDemoFileHeader não encontrado ou sem map_name")
}

// extractMapNameFromProtobufBytes busca o campo map_name (field 5) no protobuf
func (me *MapExtractor) extractMapNameFromProtobufBytes(data []byte) string {
	reader := bytes.NewReader(data)

	for {
		// Ler tag + wire type
		tag, err := binary.ReadUvarint(reader)
		if err != nil {
			break
		}

		fieldNumber := tag >> 3
		wireType := tag & 0x07

		// map_name é o field 5 no CDemoFileHeader
		if fieldNumber == 5 && wireType == 2 { // 2 = length-delimited string
			// Ler o comprimento da string
			length, err := binary.ReadUvarint(reader)
			if err != nil {
				break
			}

			// Ler a string
			mapNameBytes := make([]byte, length)
			n, err := reader.Read(mapNameBytes)
			if err != nil || n != int(length) {
				break
			}

			mapName := string(mapNameBytes)
			if me.isValidMapName(mapName) {
				return mapName
			}
		} else {
			// Skip field baseado no wire type
			switch wireType {
			case 0: // Varint
				_, err := binary.ReadUvarint(reader)
				if err != nil {
					return ""
				}
			case 1: // 64-bit
				var buf [8]byte
				_, err := reader.Read(buf[:])
				if err != nil {
					return ""
				}
			case 2: // Length-delimited
				length, err := binary.ReadUvarint(reader)
				if err != nil {
					return ""
				}
				buf := make([]byte, length)
				_, err = reader.Read(buf)
				if err != nil {
					return ""
				}
			case 5: // 32-bit
				var buf [4]byte
				_, err := reader.Read(buf[:])
				if err != nil {
					return ""
				}
			default:
				return ""
			}
		}
	}

	return ""
}

// extractFromASCII busca strings ASCII conhecidas de mapas nos dados binários
func (me *MapExtractor) extractFromASCII(data []byte) string {
	// Converter para string e buscar padrões conhecidos
	str := string(data)

	// Buscar todos os nomes de mapa conhecidos
	for mapName := range me.knownMaps {
		if strings.Contains(str, mapName) {
			return mapName
		}
	}

	// Se não encontrar um mapa conhecido, tentar extrair strings que parecem ser nomes de mapa
	return me.extractMapLikeString(data)
}

// extractMapLikeString tenta encontrar strings que parecem ser nomes de mapa (de_* ou cs_*)
func (me *MapExtractor) extractMapLikeString(data []byte) string {
	// Procurar por padrão "de_" ou "cs_" seguido de caracteres válidos
	prefixes := []string{"de_", "cs_"}

	for _, prefix := range prefixes {
		prefixBytes := []byte(prefix)
		for i := 0; i < len(data)-len(prefixBytes); i++ {
			if bytes.Equal(data[i:i+len(prefixBytes)], prefixBytes) {
				// Encontrou prefixo, agora extrair até encontrar um caractere inválido
				end := i + len(prefixBytes)
				for end < len(data) && isValidMapChar(data[end]) {
					end++
				}

				if end > i+len(prefixBytes) { // Pelo menos "de_X" ou "cs_X"
					mapName := string(data[i:end])
					if me.isValidMapName(mapName) {
						return mapName
					}
				}
			}
		}
	}

	return ""
}

// isValidMapChar verifica se um byte é válido em um nome de mapa
func isValidMapChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '_'
}

// isValidMapName valida se uma string é um nome de mapa válido
func (me *MapExtractor) isValidMapName(name string) bool {
	// Verificar se é um mapa conhecido
	if me.knownMaps[name] {
		return true
	}

	// Validação básica: começa com "de_", "cs_", "ar_" ou "dm_" e tem pelo menos 6 caracteres (ex: de_xxx)
	validPrefixes := []string{"de_", "cs_", "ar_", "dm_"}
	hasValidPrefix := false
	for _, prefix := range validPrefixes {
		if strings.HasPrefix(name, prefix) {
			hasValidPrefix = true
			break
		}
	}

	if hasValidPrefix && len(name) >= 6 {
		for _, c := range name {
			if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
				return false
			}
		}
		return true
	}

	return false
}

// ExtractMapFromStartFragment tenta extrair o mapa apenas do fragmento de início
// Este é o mais confiável pois o CDemoFileHeader geralmente vem no start fragment
func (me *MapExtractor) ExtractMapFromStartFragment(startFragment []byte) (string, error) {
	if len(startFragment) == 0 {
		return "", fmt.Errorf("start fragment vazio")
	}

	// Tentar protobuf primeiro
	mapName, err := me.extractFromProtobuf(startFragment)
	if err == nil && mapName != "" {
		return mapName, nil
	}

	// Tentar ASCII
	mapName = me.extractFromASCII(startFragment)
	if mapName != "" {
		return mapName, nil
	}

	return "", fmt.Errorf("mapa não encontrado no start fragment")
}
