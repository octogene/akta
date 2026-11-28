# Operators' console

`akta-ktor-console` gives operators web pages to find stuck sagas, understand what happened, and resolve them. The HTML is rendered by your Ktor application, and [htmx](https://htmx.org) (bundled with the module) sends the actions. Operators only need a browser, and there is no JavaScript to build.

```kotlin
dependencies {
    implementation("dev.octogene:akta-ktor-console:0.1.0")      // brings akta-core and Ktor's server core
}
```

## Mounting it

```kotlin
routing {
    authenticate("operators") {
        route("/admin/console") {
            sagaConsole(engine, actor = { call -> call.principal<UserIdPrincipal>()?.name })
        }
    }
}
```

- **Your application owns security.** The pages run inside whatever `authenticate` block you put them in. Without one, anyone who reaches them can see sagas and act on them.
- **The operator comes from your login, never from the request.** `actor` returns who is calling. Every action is recorded under that name in the saga's history. When `actor` returns `null`, the action answers `401`.
- **For a read-only console, use `sagaConsole(engine)`, without `actor`.** It offers no action and accepts none: use it where people should look but not change anything.
- **Nothing is installed on your application.** The console only adds routes: no plugin, no `ContentNegotiation`, no sessions.

## The pages

| Page | What it shows |
|---|---|
| `/` | Stuck sagas by reason, how far behind the recovery job is, the oldest unfinished saga, unfinished sagas by state |
| `/queue` | Stuck sagas, stuck longest first, grouped by reason: sagas stuck for the same cause are often handled together |
| `/search?q=` | A business key's sagas, or the sagas a trace touched. A saga id opens that saga directly |
| `/saga/{id}` | One saga: its steps as a line, what happened and what to check, the actions it accepts now, and its history |

On a saga's page:

- **The steps**: done (with what `logAs` says of the output), current, pending, undone, or never applied. You can see what an `abort` would undo before choosing it.
- **What happened**: the stuck reason in plain words, what to check in the external system, the last error, and why the saga is failing, if it is.
- **What to do**: only the actions the saga accepts now ([`engine.actions`](admin-api.md#reading)), each with what it will do, such as "Calls 'pay' again WITHOUT checking: only if you confirmed it was not applied". Every action asks for confirmation. An output is asked for only where the action needs one, and marked required when it is. Each action that isn't available is listed with the reason the engine gives.
- **The history**, with who took each manual action.

After an action, the saga's block is redrawn in place with its new state, and a line says what happened. If the engine refused the action, the line says why. The saga may have moved on since the page was loaded, or the output may not fit the step.

## Security

- **Strict Content-Security-Policy.** Every page is sent with a policy that allows scripts and styles only from the console itself, no inline code, no `eval`, and no framing (`frame-ancestors 'none'`). htmx is configured to match: it injects no style and evaluates nothing.
- **Protection against cross-site requests.** An action is accepted only when htmx sends it, because htmx adds an `HX-Request` header that a form on another site cannot set. A form posted from elsewhere answers `403`. Combine this with your authentication's own protection, such as `SameSite` cookies.
- **Everything is escaped.** The pages are built with kotlinx.html, which escapes every text and attribute. An error message holding markup is shown as text.
- **No personal data.** A saga's input and its step outputs are never shown, only what the history already shows: the references `logAs` returns, errors and reasons.

## The JSON API

The console and the [admin API](admin-api.md) rely on the same engine calls (`actions`, `steps`, `history`), so both offer the same actions. Use the API to build your own screen, or to script operations.
