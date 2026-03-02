# LoL Draft Analyzer - Roadmap du Projet

## De 53% à 88% de précision : Comment j'ai construit un modèle de prédiction de victoire basé sur le draft

---

## Le Problème Initial

Prédire quelle équipe va gagner une partie de League of Legends uniquement à partir de la composition des équipes (la "draft") - sans aucune information sur le déroulement de la partie.

---

## Phase 1 : Les Fondations (Accuracy: ~53%)

### Objectif
Créer une baseline fonctionnelle avec les données brutes.

### Ce qui a été fait
- **Collecte de données** via l'API Riot Games
  - Joueurs Diamond I sur le serveur EUW
  - ~8000+ matchs collectés
- **Features de base**
  - 10 champions encodés (5 par équipe)
  - One-hot encoding des champions

### Résultat
- **53% d'accuracy** - À peine mieux que le hasard
- Le modèle ne capture pas les interactions entre champions

### Leçon apprise
Les champions seuls ne suffisent pas. Il faut capturer les **relations** entre eux.

---

## Phase 2 : Correction du Data Leakage (Accuracy: 98% → 53%)

### Le problème découvert
Le modèle initial avait 98% d'accuracy... trop beau pour être vrai !

**Data Leakage identifié :**
- Utilisation de stats post-game (kills, gold, damage)
- Ces infos ne sont pas disponibles avant la partie

### Solution
- Suppression de toutes les features post-game
- Conservation uniquement des infos de draft

### Résultat
- Retour à **53% d'accuracy** - mais cette fois c'est honnête !

---

## Phase 3 : Champion Win Rates (Accuracy: 53% → 70%)

### Hypothèse
Certains champions sont objectivement plus forts que d'autres dans le meta actuel.

### Features ajoutées
- **Win rate individuel** de chaque champion (calculé sur nos données)
- **Win rate moyen** de l'équipe
- **Différence de win rate** entre les équipes

### Résultat
- **70% d'accuracy** (+17 points !)
- Le win rate devient la feature la plus importante

---

## Phase 4 : Composition d'équipe (Accuracy: 70% → 78%)

### Hypothèse
La diversité et l'équilibre d'une équipe comptent.

### Features ajoutées
- **Diversité des classes** (Tank, Mage, Assassin, etc.)
- **Équilibre des dégâts** (physique vs magique)
- **Nombre de tanks, assassins, mages** par équipe
- **Ratios de composition**

### Résultat
- **78% d'accuracy** (+8 points)
- Les compos déséquilibrées (full AD, 0 tank) sont pénalisées

---

## Phase 5 : Synergies entre Champions (Accuracy: 78% → 88%)

### Hypothèse
Certaines combinaisons de champions sont plus fortes ensemble.

### Features ajoutées

#### Synergies prédéfinies (70+ paires)
- **Bot lane combos** : Lucian+Nami, Kai'Sa+Nautilus, etc.
- **Knockup synergies** : Yasuo+Malphite, Yasuo+Diana, etc.
- **Engage combos** : Jarvan+Orianna, etc.

#### Synergies data-driven
- Calcul automatique des paires gagnantes depuis les données

#### Détection de compositions
- **Knockup comp** : équipe avec Yasuo/Yone + sources de knockup
- **Engage comp** : multiples champions d'engage
- **Poke comp** : Jayce, Xerath, Ziggs ensemble
- **Protect the carry** : hypercarry + supports/tanks

### Résultat
- **88.3% d'accuracy** (+10 points !)
- `synergy_score_diff` devient la 2ème feature la plus importante

---

## Phase 6 : Collecte High Elo (En cours)

### Objectif
Améliorer la qualité des données en collectant des matchs de plus haut niveau.

### Implémentation
- Support pour **Challenger** (~300 joueurs)
- Support pour **Grandmaster** (~700 joueurs)
- Support pour **Master** (~10,000 joueurs)
- Conservation de **Diamond I** pour le volume

### Commande
```bash
python src/collect_data_safe.py --high-elo-only
```

---

## Insights Découverts

### Top 5 Synergies (Win Rate)
| Paire | Win Rate | Matchs |
|-------|----------|--------|
| Kai'Sa + Thresh | 57.9% | 190 |
| Jinx + Thresh | 56.8% | 162 |
| Ezreal + Karma | 56.5% | 147 |
| Lucian + Nami | 55.9% | 420 |
| Xayah + Rakan | 55.2% | 380 |

### Pires Synergies (Win Rate)
| Paire | Win Rate | Matchs |
|-------|----------|--------|
| Yasuo + Lee Sin | 45.2% | 156 |
| Zed + Graves | 46.1% | 132 |

---

## Stack Technique

- **Langage** : Python 3.x
- **API** : Riot Games API (League of Legends)
- **ML** : scikit-learn, XGBoost
- **Data** : pandas, numpy
- **Features** : 50+ features engineered

---

## Évolution de l'Accuracy

```
53% ──────┐
          │ Data Leakage Fix
53% ──────┤
          │ Champion Win Rates (+17%)
70% ──────┤
          │ Team Composition (+8%)
78% ──────┤
          │ Champion Synergies (+10%)
88% ──────┘
```

---

## Prochaines Étapes Potentielles

1. **Counter-picks** : Détecter les matchups défavorables
2. **Meta-adaptation** : Pondérer par le patch actuel
3. **Role-specific** : Analyser les synergies par lane
4. **Deep Learning** : Tester des architectures plus complexes

---

## Conclusion

En partant d'un simple encodage de champions (53%), nous avons construit un modèle capable de prédire le vainqueur d'une partie avec **88% de précision** uniquement à partir du draft.

Les clés du succès :
1. **Feature engineering** intelligent
2. **Compréhension du domaine** (LoL)
3. **Itération** et validation rigoureuse

---

*Projet réalisé dans le cadre de DataScientest*
*Données collectées via l'API Riot Games*
