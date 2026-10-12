# Monterrico Bot Cloud · Capturador MVP 0.2

Lee resultados oficiales de Monterrico de los canales STOMP observados en `https://wager.telepodromo.net/wager`, construye mensajes y **los guarda en Supabase PostgreSQL**. Aún **no envía mensajes a WhatsApp**. Diseñado para alojar un único worker persistente en Railway con repositorio GitHub.

## Arquitectura

`Telepódromo → STOMP/SockJS → Worker Python (Railway) → tablas privadas Supabase → (futuro: envío WhatsApp)`.

El catálogo `/broker/v2.tracks.es-PR` contiene un objeto de hipódromo `{id: 690, country: "PE"}` y cartillas llamadas `Monterrico`. De ahí se extraen los ID de cartilla y cada carrera. El bot escucha:

- `/broker/v3.race.{raceId}.es-PR` (estado `OFFICIALLY_COMPLETED`)
- `/broker/v2.race.{raceId}.es-PR.result` (`complete: true`, `runnersResults`, `exoticBetsResults`)

Se guardan solo los nuevos resultados oficiales detectados después de observar la carrera como pendiente. Las ya terminadas al primer inicio no se republican; los registros persisten en Supabase. Se guarda hasta el 5.º lugar **si el servidor lo publica**; no se inventan posiciones.

**Avisos:** el código usa una URL de transporte SockJS *tentativa* deducida de `app.config.js` y mensajes capturados por Chrome. La conexión real todavía NO se ha probado contra el servidor; puede necesitar ajuste del `MON_WS_URL` o la negociación STOMP. Tampoco se han probado actualizaciones en vivo tras finalizar carreras. Es una integración pública observada, no una API oficial documentada ni un acuerdo de redistribución.

## A. Supabase (unos minutos)

1. Crear un proyecto Supabase.
2. Abrir **SQL Editor** y ejecutar `db/schema.sql` completo (tablas `observed_races`, `result_outbox`, RLS habilitado).
3. En **Connect**, copiar el URI PostgreSQL de **Session pooler** (puerto 5432) si Railway no llega por IPv6. Usar **Direct connection** si el hosting tiene IPv6. Copiar la cadena real desde Supabase; **no construirla a mano**.
4. Guardar esa cadena únicamente en Railway como secreto `DATABASE_URL`, agregando `sslmode=require` según proceda. Nunca dejarla en el repositorio ni en variables públicas de Vercel.

## B. GitHub

1. Crear un repositorio **privado** llamado `monterrico-bot`.
2. Subir el contenido de **esta carpeta** a la raíz del repo, conservando `Dockerfile`, `main.py`, `storage.py`, `requirements.txt` y `db/schema.sql`.
3. `.gitignore` excluye bases locales y `.env`.

## C. Railway (worker siempre encendido)

1. Crear cuenta/proyecto en Railway, seleccionar **New → GitHub Repo** y el repositorio `monterrico-bot`.
2. Railway detectará el `Dockerfile` de la raíz y ejecutará `python main.py --live`.
3. En Railway → servicio → **Variables** agregar:
   - `DATABASE_URL`: URI privado de Supabase.
   - `MON_WS_SERVER_ID=000`: predeterminado; solo cambiarlo si DevTools muestra otro segmento de 3 dígitos.
   - `MON_RECONNECT_SECONDS=1800`: reconexión cada media hora para refrescar cartillas.
   - Opcional `MON_WS_URL`: la dirección `wss://...` verificada en Chrome (un `sessionId` fijo puede expirar; se recomienda ajustar la ruta generadora).
4. Asegurar **una única réplica** del worker, sin cron ni suspensiones por inactividad. Este worker no atiende peticiones HTTP y no necesita un dominio público.
5. Revisar logs. Los mensajes encolados aparecerán en Supabase → Table Editor → `result_outbox` con `state=queued`.
6. Programar una prueba con una nueva carrera de Monterrico. Validar conexión, cambio de estado, latencia y que no haya duplicados; no habilitar WhatsApp hasta pasar la prueba.

## D. Prueba local sin Internet ni WhatsApp

```bash
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python main.py --demo
```

El modo `--demo` utiliza SQLite local (`monterrico-demo.sqlite3`) y **no** lee `DATABASE_URL`. La segunda ejecución no vuelve a guardar la misma carrera. El modo `--live` exige `DATABASE_URL`.

## E. Registro y seguridad

`observed_races`: carreras observadas mientras estaban pendientes. `result_outbox`: cada resultado oficial nuevo, mensaje, hash y estado de entrega (por ahora `queued`). La clave `race_id` evita duplicados en la base; si el servidor modifica un resultado ya guardado, el bot lo registra en los logs para revisión manual.

RLS está activado en ambas tablas, sin políticas de lectura pública. El worker accede con conexión PostgreSQL del servidor. No exponer contraseñas de BD ni tokens en commits, capturas o mensajes.

**Pendientes:** validar ruta real de la conexión WebSocket, recepción en tiempo real, permiso de reutilización pública de los datos de Telepódromo y la integración final de WhatsApp. Para un grupo >200 integrantes, WhatsApp Web automatizado no es un mecanismo oficial ni ofrece garantías de continuidad y puede causar restricciones de cuenta. El backend queda listo para incorporar posteriormente un emisor permitido o un modo asistido.
