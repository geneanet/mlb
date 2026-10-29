# MLB - Instructions pour les Agents IA & Développeurs

MLB (Modular Load Balancer) est un répartiteur de charge L4/L7 haute performance écrit en Go 1.26, conçu pour une haute disponibilité avec rechargements sans coupure (*zero-downtime reloads*).

---

## 1. Architecture & Modèle Modulaire

MLB repose sur une architecture en pipeline articulée autour de 4 types de modules :

1. **`backends_inventory`** : Découverte des cibles amont (ex. `consul`, `static`).
2. **`backends_processor`** : Enrichissement des métadonnées et bilans de santé (ex. `mysql`, `redis`, `consul_kv`, `simple_filter`).
3. **`balancer`** : Algorithmes de sélection de backend (ex. `wrr` pour Smooth Weighted Round-Robin, `wlc` pour Weighted Least Connections).
4. **`proxy`** : Écoute frontale et routage réseau vers le backend sélectionné (ex. `tcp`, `redis`, `memcache`).

### Pattern d'enregistrement des modules
Chaque module s'enregistre automatiquement auprès du registre central dans sa fonction `init()` via `module.RegisterFactory` :
```go
func init() {
    module.RegisterFactory(category, typeName, newFn, validateFn)
}
```
- `category` : `"backends_inventory"`, `"backends_processor"`, `"balancer"` ou `"proxy"`.
- `typeName` : Identifiant de type utilisé dans la configuration HCL (ex. `"consul"`, `"wrr"`, `"redis"`).
- `newFn` : `func(config *module.Config, wg *sync.WaitGroup, ctx context.Context) (any, error)`
- `validateFn` : `func(config *module.Config) hcl.Diagnostics`

Les modules peuvent optionnellement implémenter :
- `module.ReadyReporter` (`Ready() <-chan struct{}`) pour signaler qu'ils sont prêts.
- `module.Binder` (`Bind(modules module.ModulesRegistry) error`) pour résoudre les dépendances vers d'autres modules après instanciation.

Les packages des modules sont importés pour leurs effets de bord (`_ "mlb/..."`) dans `main.go`.

### Contrats d'interfaces & Flux inter-modules
La communication entre modules repose sur les interfaces définies dans le package `mlb/backend` :

| Rôle | Interface | Implémenté par | Consommé par | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Source d'événements** | `backend.BackendUpdateProvider` | Inventaires, Processeurs | Processeurs, Balancers, Proxies (`redis`) | Diffuse les ajouts/modifications/suppressions de backends via `ProvideUpdates(s)`. |
| **Récepteur d'événements** | `backend.BackendUpdateSubscriber` | Processeurs, Balancers, Proxies (`redis`) | Inventaires, Processeurs | Reçoit les événements de cycle de vie via `ReceiveUpdate(u)`. |
| **Sélection unitaire** | `backend.BackendProvider` | Balancers | Proxies (`tcp`) | Fournit un backend pour une connexion cliente via `GetBackend(wait)`. |
| **Liste complète** | `backend.BackendListProvider` | Inventaires, Balancers | Proxies (`memcache`), Dashboard | Fournit l'ensemble des cibles actives via `GetBackendList()`. |

**Résolution des dépendances (`module.Binder`) :**
Dans la méthode `Bind(modules module.ModulesRegistry)`, récupérer le module amont configuré par son identifiant à l'aide de la fonction générique typée :
```go
m, err := module.Get[backend.BackendProvider](modules, b.source)
if err != nil {
    return err
}
```

---

## 2. Commandes de Développement & Validation

Toute modification doit être validée à l'aide des commandes suivantes :

```bash
# Compilation du binaire
go build -o mlb .

# Validation syntaxique et sémantique d'un fichier de configuration
./mlb -config config.example.hcl -configtest

# Exécution ciblée d'un package (recommandé pendant le développement)
go test -v -race ./proxy/redis/...

# Exécution complète des tests unitaires et couverture
go test -v -cover ./...

# Détection des courses de données (data races) - OBLIGATOIRE sur toute modification concurrente/réseau
go test -race ./...

# Formatage du code
go fmt ./...

# Analyse statique et linter
golangci-lint run ./...
# Si golangci-lint n'est pas disponible :
go vet ./...
```

---

## 3. Concurrence, Réseau & Stabilité

La stabilité d'un répartiteur de charge haute disponibilité repose sur des principes stricts de conception :

### Écoute réseau & Rechargement sans coupure (`system.Listen`)
- **Règle absolue** : Toute socket réseau en écoute (TCP ou socket UNIX) doit être créée via `system.Listen(network, address)` et **jamais** directement via `net.Listen`.
- `system.Listen` assure l'héritage transparent des descripteurs de fichiers via `tableflip` lors d'un rechargement sans coupure (`SIGHUP`) et supprime automatiquement les sockets UNIX résiduelles orphelines.

### Encapsulation de l'état (Pas d'état global mutable)
- **Proscrire les variables globales mutables** au niveau des packages (ex. tailles de buffers, configurations partagées).
- L'état de fonctionnement et la configuration doivent être strictement encapsulés dans les instances des structures de chaque composant (proxy, balancer, pool, etc.).

### Synchronisation & Verrous
- Protéger systématiquement les accès concurrents aux structures partagées (maps, registres de backends, compteurs).
- Utiliser `sync.RWMutex` avec rigueur : poser un `RLock()` pour toute lecture concurrente (y compris lors de la sérialisation JSON ou de l'itération).
- Éviter les deadlocks en imposant un ordre d'acquisition déterministe en cas de verrous multiples imbriqués.

### Gestion du cycle de vie mémoire & `sync.Pool`
- Ne jamais réinsérer un buffer ou un objet dans un `sync.Pool` s'il peut encore être référencé, lu ou comparé concurremment (prévention absolue des *Use-After-Free*).
- Libérer les ressources acquises via `defer` ou explicitement après achèvement complet des opérations de traitement et de comparaison.

### Robustesse réseau & Gestion des erreurs
- **Interdiction formelle de `panic()`** comme mécanisme de contrôle de flux ou lors d'erreurs réseau transitoires (ex. échec temporaire d'un `listener.Accept()`, fermeture d'une socket distante).
- Gérer les erreurs réseau idiomatiquement par logging structuré et mécanismes de backoff progressif temporaire.

### Cycle de vie des Goroutines & Contextes
- Lier toute goroutine d'arrière-plan à un `context.Context` d'annulation ou à des canaux de terminaison (`stopChan`).
- S'assurer que les routines d'écoute et de travail se terminent proprement lors de l'arrêt ou du rechargement à chaud (`tableflip`).

---

## 4. Conventions de Code & Tests

### Tests & Utilitaires (`mlb/testutil`)
- **Bibliothèque standard exclusivement** : Utiliser uniquement le package standard `testing` avec des assertions explicites (`if got != want { t.Errorf(...) }` / `t.Fatalf`). Ne pas importer de bibliothèques tierces d'assertion (ex. testify).
- **Attente asynchrone sans `time.Sleep`** : Proscrire les temporisations arbitraires `time.Sleep(...)` dans les tests asynchrones. Utiliser `testutil.Eventually(t, condition, timeout, tick)` ou `testutil.Consistently(t, condition, duration, tick)` pour garantir la rapidité et la stabilité des tests.
- **Parsing HCL unitaire** : Utiliser `testutil.ParseHCL(t, snippet)` pour parser des fragments HCL en mémoire sans créer de fichiers temporaires.
- **Mocks d'événements** : Utiliser `testutil.DummyProvider` et `testutil.DummySubscriber` pour simuler et vérifier les flux d'événements de backends en isolation.
- **Isolation & Déterminisme** : Les tests ne doivent pas laisser de goroutines orphelines écrivant sur `t.Log` après la fin du test.

### Conventions de Commit (Conventional Commits)
Le projet applique la convention *Conventional Commits* pour l'historique Git :
- Format : `<type>(<scope>): <description>` (ex. `fix(redis): ...`, `perf(memcache): ...`, `feat(balancer): ...`, `docs(hcl): ...`).
- Types usuels : `fix`, `feat`, `perf`, `docs`, `test`, `refactor`.

### Journalisation structurée
- Utiliser `github.com/rs/zerolog` (`log.With().Str("id", ...)`).
- Conserver une cohérence dans les champs structurés (`id`, `address`, `peer`, `error`).

### Style & Documentation
- Commenter chaque fonction, type et package exporté selon les idiomes Go (`// NomDuType ...`).
- Documenter le *pourquoi* des arbitrages complexes via des commentaires ciblés.

---

## 5. Synchronisation Configuration & Documentation

Le système de configuration repose sur HashiCorp HCL (`hcl/v2`).

Toute modification (ajout, renommage, suppression) d'un paramètre ou bloc de configuration doit être **impérativement répercutée en synchronisation** sur :
1. Le schéma et décodeur HCL du code Go (`config/` ou module ciblé).
2. Le fichier d'exemple global `config.example.hcl`.
3. La documentation correspondante dans le répertoire `docs/` (`docs/configuration.md`, `docs/backends_*.md`, etc.).
