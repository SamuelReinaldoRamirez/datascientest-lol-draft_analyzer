# Options pour un Assistant de Draft en Temps Réel

## Problème Actuel

Le modèle actuel prédit le résultat **uniquement après** que les 10 champions soient sélectionnés. Cela limite son utilité pendant le processus de draft.

---

## Option 1 : Prédiction avec Draft Partielle

**Concept** : Compléter les slots vides avec des valeurs moyennes/neutres pour permettre une prédiction anticipée.

```python
def predict_partial_draft(known_picks, remaining_slots):
    """
    Pour chaque slot vide:
    - Utiliser le winrate moyen global (50%)
    - Ou utiliser des features "neutres" (synergies = 0, matchups = 0.5)
    """
```

**Avantages** :
- Simple à implémenter
- Utilise le modèle existant

**Inconvénients** :
- Précision réduite avec peu de picks
- Ne donne pas de recommandations concrètes

---

## Option 2 : Système de Recommandation de Pick

**Concept** : Pour chaque position restante, évaluer tous les champions disponibles et recommander les meilleurs.

```python
def recommend_next_pick(current_draft, team_side, position):
    """
    Pour chaque champion disponible:
    1. Simuler la draft avec ce pick
    2. Calculer: synergies avec alliés, matchup vs adversaire, counters potentiels
    3. Retourner le top 5 des meilleurs choix avec scores

    Returns:
        [
            {'champion': 'Jinx', 'score': 0.75, 'reasons': ['synergie Lulu', 'safe pick']},
            {'champion': 'Kai\'Sa', 'score': 0.72, 'reasons': ['flex pick', 'synergie Nautilus']},
            ...
        ]
    """
```

**Avantages** :
- Conseils concrets et actionnables
- Explications des choix (synergies, counters)
- Utilisable à chaque étape du draft

**Inconvénients** :
- Plus complexe à implémenter
- Nécessite d'évaluer ~150 champions par décision

---

## Option 3 : Modèles par Phase de Draft

**Concept** : Entraîner plusieurs modèles spécialisés pour différentes étapes du draft.

| Phase | Picks connus | Modèle |
|-------|--------------|--------|
| Post-bans | 0 picks | `model_phase_0.pkl` |
| Round 1 | 2 picks | `model_phase_1.pkl` |
| Round 2 | 4 picks | `model_phase_2.pkl` |
| Round 3 | 6 picks | `model_phase_3.pkl` |
| Round 4 | 8 picks | `model_phase_4.pkl` |
| Complet | 10 picks | `model_phase_5.pkl` |

**Avantages** :
- Prédictions optimisées pour chaque phase
- Meilleure précision que l'Option 1

**Inconvénients** :
- 6 modèles à entraîner et maintenir
- Nécessite plus de données pour chaque phase
- Complexité accrue

---

## Option 4 : Score de Draft Incrémental

**Concept** : Calculer un score de draft basé uniquement sur les features disponibles (sans prédiction ML).

```python
def calculate_draft_score(current_draft):
    """
    Score basé sur:
    - Synergies connues entre alliés pickés
    - Counters détectés vs adversaires pickés
    - Équilibre de la composition (AP/AD, engage, peel)
    - Flexibilité des picks (multi-rôles)

    Returns:
        {
            'team_100_score': 0.55,
            'team_200_score': 0.45,
            'warnings': ['Pas de frontline', 'Full AD'],
            'advantages': ['Forte synergie bot', 'Bon matchup mid']
        }
    """
```

**Avantages** :
- Pas besoin de ML, utilise les dictionnaires existants
- Rapide et explicable
- Fonctionne avec n'importe quel nombre de picks

**Inconvénients** :
- Moins précis qu'un modèle ML
- Basé sur des règles heuristiques

---

## Option 5 : Hybrid (Recommandé)

**Concept** : Combiner les options 2 et 4 pour un assistant complet.

### Composants :

1. **Score de Draft en temps réel** (Option 4)
   - Affiche le score actuel à chaque pick
   - Alerte sur les problèmes (full AD, pas de CC, etc.)

2. **Recommandations de picks** (Option 2)
   - Top 5 champions pour chaque position
   - Raisons expliquées (synergies, counters, winrate)

3. **Prédiction finale** (modèle existant)
   - Prédiction précise une fois le draft complet

### Architecture :

```
DraftAssistant
├── LiveScoreCalculator      # Score incrémental
├── PickRecommender          # Suggestions de champions
├── CounterDetector          # Alertes matchups
├── SynergyAnalyzer          # Synergies en cours
└── WinPredictor             # Modèle ML final
```

---

## Comparaison

| Option | Complexité | Précision | Utilité en draft |
|--------|------------|-----------|------------------|
| 1. Draft partielle | Faible | Moyenne | Faible | 1-2h |
| 2. Recommandations | Moyenne | Haute | Haute |
| 3. Multi-modèles | Haute | Haute | Moyenne |
| 4. Score incrémental | Faible | Moyenne | Haute |
| 5. Hybrid | Moyenne | Haute | Très haute |

---

## Prochaines Étapes

1. **Choisir une option** à implémenter en priorité
2. **Option 4** recommandée comme premier pas (rapide, utile)
3. **Option 2** comme évolution naturelle
4. **Option 5** comme objectif final
