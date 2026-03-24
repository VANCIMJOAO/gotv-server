# gotv-server

Servidor WebSocket em Go para streaming de dados de partidas CS2 em tempo real.

Parte do ecossistema [ORBITAL ROXA](https://github.com/VANCIMJOAO/orbital-cs2).

---

## O que faz

Recebe eventos do [MatchZy](https://github.com/shobhit-pathak/MatchZy) (plugin de servidor CS2) e transmite para clientes conectados via WebSocket. Os dados alimentam o scoreboard ao vivo do ORBITAL.

## Stack

- **Go** com gorilla/websocket
- **Supabase** para persistência
- **Railway** para deploy

## Endpoints

| Rota | Método | Descrição |
|------|--------|-----------|
| `/ws` | WebSocket | Conexão para receber eventos em tempo real |
| `/api/matchzy/events` | POST | Webhook que recebe eventos do MatchZy |
| `/health` | GET | Health check |

## Eventos transmitidos

- `round_start` / `round_end`
- `player_death`
- `bomb_planted` / `bomb_defused`
- `match_start` / `match_end`
- Atualizações de score e estatísticas

## Variáveis de ambiente

```
SUPABASE_URL=
SUPABASE_KEY=
PORT=8080
```

## Rodar localmente

```bash
go run main.go
```

---

Desenvolvido para uso nos campeonatos organizados pelo ORBITAL ROXA.
