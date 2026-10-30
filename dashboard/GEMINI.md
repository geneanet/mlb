# Dashboard MLB — Instructions d'Architecture & Intentions de Conception

Ce document détaille les principes de conception, l'architecture technique et les règles impératives régissant le dashboard web de MLB (`dashboard/index.html` et `dashboard/dashboard.go`).

---

## 1. Principes Fondamentaux & Contraintes

1. **Zéro Dépendance Externe (*Zero External Dependencies*)** :
   - Aucun framework JS (pas de React, Vue, Svelte).
   - Aucune bibliothèque CSS externe (pas de Tailwind, Bootstrap).
   - Aucune bibliothèque de graphes (pas de D3, Cytoscape, Dagre, Graphlib).
   - Aucune police ou ressource externe (aucun appel CDN Google Fonts, FontAwesome, etc.).
   - Le dashboard doit fonctionner parfaitement sur des serveurs isolés ou en environnement *air-gapped* sans accès Internet.
   - Les tests unitaires Go (`TestDashboardAssetsNoExternalDependencies`) veillent à l'absence stricte de CDN ou bibliothèques tierces.
2. **Single-File Asset Embarqué** :
   - L'interface complète (HTML, styles CSS, scripts JS, icônes vectorielles SVG) réside dans un fichier unique `dashboard/index.html` embarqué dans le binaire Go via `//go:embed index.html`.
3. **Robustesse & Tolérance aux Pannes** :
   - L'interface ne doit jamais planter sur des données partielles, temporairement indisponibles ou des backends `null`.
   - Tolérance aux rechargements sans coupure (*zero-downtime reloads*) et aux déconnexions réseau avec reconnexion automatique.

---

## 2. Architecture Frontend (`dashboard/index.html`)

### A. État Global (`state`)
L'état de l'application est centralisé dans un objet unique `state` :
- `topology` : graphe des modules découvert via `/topology`.
- `backends` : dictionnaire des backends groupés par fournisseur via `/backends`.
- `proxyMetrics` : statistiques des proxys via `/proxy_metrics`.
- `selectedNodeId` : nœud inspecté dans le panneau latéral (drawer).
- `activeTab` : onglet actif (`'topology'`, `'backends'`, `'metrics'`).
- `transform` : coordonnées (`x`, `y`) et échelle (`scale`) du graphe SVG.
- `drawerWidth` : largeur courante du panneau latéral (valeur par défaut : `600px`).
- `drawerFilterQuery` : filtre textuel local aux backends du module inspecté.
- `globalSearchQuery` : filtre textuel de l'onglet général des backends.
- `categoryFilter` : filtre de catégorie (`'all'`, `'backends_inventory'`, `'backends_processor'`, `'balancer'`, `'proxy'`).

### B. Moteur de Topologie DAG & Réduction des Croisements (Sugiyama)
La topologie est projetée sous forme de graphe orienté acyclique (DAG) vectoriel SVG via `computeDAGLayout(nodes)` :
1. **Composantes connexes indépendantes** :
   - Découverte des sous-graphes indépendants (pipelines de services disjoints) via parcours en largeur.
   - Chaque composante est disposée dans sa propre bande horizontale avec espacement (`compGap = 56px`) pour éliminer tout croisement inter-pipelines.
2. **Attribution des Rangs Topologiques (`getRank`)** :
   - Calcul de la profondeur relative avec détection de cycles via `recursionStack`.
   - Regroupement des nœuds par colonnes de même rang.
3. **Réduction des croisements (Heuristique Barycentrique)** :
   - 6 passes alternées de balayage avant (*forward sweep*, alignement avec les sources amont) et de balayage arrière (*backward sweep*, alignement avec les consommateurs aval).
4. **Arêtes en Courbes de Bézier Cubiques** :
   - Tracé fluide via `M x1 y1 C (x1+dx) y1, (x2-dx) y2, x2 y2` avec flèches orientées (`marker-end`).
5. **Troncature des Noms de Cartes SVG** :
   - Les libellés de modules de plus de 17 caractères sont tronqués avec une ellipse (`…`) et accompagnés d'une balise SVG `<title>` pour afficher le nom complet au survol.

### C. Pan & Zoom Interactif
- **Déplacement (Pan)** :
  - Restreint exclusivement au bouton gauche (`e.button === 0`). Le clic droit (menu contextuel / inspection) ne doit jamais déclencher de pan.
  - Gestion conjointe de `pointerup` et `pointercancel` pour libérer la capture du pointeur et réinitialiser `isPanning`.
- **Zoom** :
  - À la molette : centré sur la position instantanée du curseur souris, borné entre `0.25` et `3.0`.
  - Boutons flottants `+` / `−` : centrés sur le milieu du conteneur.

### D. Surbrillance Hiérarchique des Flux (`highlightNodeFlow`)
Au survol d'un module :
- **Flux direct** (`flow-direct`, bordure et ombre renforcées) : parent direct amont et enfants directs aval.
- **Flux indirect** (`flow-indirect`, lueur douce) : remonte tous les ancêtres jusqu'à l'inventaire racine, et descend tous les successeurs jusqu'aux proxys frontaux.
- Les autres arêtes sont atténuées (`dimmed`).

### E. Panneau Latéral d'Inspection (Module Drawer)
- Largeur par défaut fixée à **`600px`** (`max-width: 90vw;`), redimensionnable interactivement via `#drawer-resizer`.
- **Cycle de vie du filtre** : `state.drawerFilterQuery` et `#drawer-backend-filter.value` doivent impérativement être réinitialisés à l'ouverture (`openDrawer`) et à la fermeture (`closeDrawer`).
- **Coloration syntaxique HCL autonome (`highlightHCL`)** :
  - Tokeniseur regex sans dépendance externe identifiant blocs, types, identifiants, chaînes, booléens, fonctions et opérateurs.
  - Échappement HTML strict via `escapeHtml()` pour parer à toute injection XSS.

### F. Navigation & Header Découplé
- **Centrage de la barre d'onglets** :
  - `.tabs-nav` est positionné de manière absolue (`position: absolute; left: 50%; transform: translateX(-50%)`).
  - Ce centrage géométrique strict le découple complètement des variations de largeur des blocs gauche (`.header-brand`) et droite (`.header-actions`).
  - Repasse en flux normal sous `768px` via `@media (max-width: 768px)` pour éviter les chevauchements sur petit écran.
- **Stabilisation du statut de synchronisation** :
  - `.refresh-status` possède une largeur minimale (`min-width: 85px; text-align: right; white-space: nowrap;`) pour supprimer les micro-saccades lors du changement de texte (*« à l'instant »*, *« il y a 12s »*).

### G. Système de Thème (Sombre / Clair / Auto)
- Thème par défaut : Sombre (standard infrastructure & observabilité).
- Sélecteur de mode : cycle Sombre ➔ Clair ➔ Auto via `cycleTheme()`. Persistance dans `localStorage ('mlb_theme')`.
- Variables CSS dédiées pour garantir les contrastes WCAG en thème clair :
  - Variables de badges : `--badge-inventory-bg`, `--badge-processor-color`, etc.
  - Variables de glow : `--success-glow`, `--warning-glow`, `--danger-glow`.
- **Règle CSS impérative** :
  - Ne **jamais** placer une directive `@media` à la suite d'un sélecteur séparé par une virgule (ex: `selecteur, @media ...`). Cela invalide tout le bloc pour le moteur CSS du navigateur. Chaque bloc `@media` doit être autonome.

---

## 3. Architecture Backend Go (`dashboard/dashboard.go`)

### Endpoints JSON
- **`GET /backends`** :
  - Agrège les cibles de tous les modules implémentant `backend.BackendListProvider` (`module.Filter[backend.BackendListProvider](ml)`).
  - Tri déterministe par adresse avec tolérance explicite aux entrées `nil`.
- **`GET /topology`** :
  - Exporte l'ensemble des modules instanciés (`TopologyNode` : ID, catégorie, type, nom, HCL source, source amont).
- **`GET /proxy_metrics`** :
  - Collecte les métriques synthétiques via `prometheus.DefaultGatherer.Gather()`.
- **`GET /dashboard` et `GET /`** :
  - Distribue la SPA embarquée `index.html`.

---

## 4. Règles d'Or pour les Évolutions Futures

1. **Sécurité contre les accès nuls (`TypeError`)** :
   - Dans le traitement des listes de backends, filtrer systématiquement les éléments nuls et sécuriser l'accès aux adresses :
     ```javascript
     let list = [...(state.backends[pId] || [])]
         .filter(b => b != null)
         .sort((a, b) => (a.address || '').localeCompare(b.address || ''));
     ```
2. **Sécurité des animations et timers** :
   - Toute boucle `requestAnimationFrame` doit avoir une condition d'arrêt immédiate si l'élément hôte est masqué (`display: none`) ou si l'onglet n'est pas actif.
3. **Synchronisation des filtres** :
   - Si une nouvelle catégorie de module fournit des backends (comme les proxies récents), s'assurer que les filtres de la barre de boutons (pills) disposent du bouton correspondant (`setCategoryFilter('proxy', this)`).
4. **Tests de non-régression (`dashboard/dashboard_test.go`)** :
   - Toute modification de structure DOM ou de logique critique doit être accompagnée d'une vérification dans `dashboard_test.go` (`TestDashboardEssentialDOM`, `TestDashboardFixesAndRobustness`).
