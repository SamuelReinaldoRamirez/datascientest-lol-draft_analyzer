# models/

Modèles entraînés pour la prédiction de victoire.

## Modèles actifs (V3)

| Fichier | Accuracy | AUC-ROC | Algo | Features |
|---|---|---|---|---|
| `model_draft.pkl` | 54.0% | 0.555 | XGBoost | 153 |
| `model_at5.pkl` | 65.4% | 0.719 | LightGBM | 171 |
| `model_at10.pkl` | 72.0% | 0.797 | XGBoost | 186 |
| `model_at15.pkl` | 78.0% | 0.864 | XGBoost | 186 |
| `model_at20.pkl` | 79.9% | 0.885 | XGBoost | 186 |

## Archives

| Dossier | Description |
|---|---|
| `backup_v2/` | Modèles V2 (avant summoner stats temporelles) |
| `archive_v1/` | Modèles V1 (premiers essais, encodage ordinal) |
