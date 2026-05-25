"""
4_Modeling — Train/compare classifiers for user-activity segmentation.
Target: activity_label  (Low / Medium / High)
"""
import os, json, joblib, warnings
warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import StratifiedKFold, cross_val_score, cross_val_predict, train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.pipeline import Pipeline
from sklearn.dummy import DummyClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import (accuracy_score, classification_report, confusion_matrix,
                              f1_score, precision_score, recall_score)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
MOD_DIR = os.path.dirname(__file__)
RES_DIR = os.path.join(MOD_DIR, "results")
os.makedirs(RES_DIR, exist_ok=True)
CLEAN_PATH = os.path.join(ROOT, "Submission", "2_Data_Cleaning", "cleaned_data.csv")

df = pd.read_csv(CLEAN_PATH)

FEATURES = ["total_transactions","months_active","tx_per_month","avg_amount",
            "median_amount","std_amount","total_amount_in","total_amount_out",
            "n_in","n_out","send_receive_ratio","weekend_ratio",
            "avg_balance","tx_velocity","n_tx_types"]
df = df.dropna(subset=FEATURES + ["activity_label"]).reset_index(drop=True)

X = df[FEATURES].values
y_raw = df["activity_label"].values
le = LabelEncoder().fit(["Low","Medium","High"])
# preserve only known classes
mask = np.isin(y_raw, le.classes_)
X, y_raw = X[mask], y_raw[mask]
y = le.transform(y_raw)
class_names = list(le.classes_)
print("Class distribution:", dict(zip(*np.unique(y_raw, return_counts=True))))
print("Shape:", X.shape)

# small sample → leave-one-out style CV via Stratified 5-fold (bounded by min class)
min_class = pd.Series(y).value_counts().min()
n_splits = min(5, min_class)
cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)

models = {
    "Baseline (most-frequent)": DummyClassifier(strategy="most_frequent"),
    "Logistic Regression": Pipeline([("sc", StandardScaler()),
        ("clf", LogisticRegression(max_iter=2000, multi_class="auto"))]),
    "K-Nearest Neighbors": Pipeline([("sc", StandardScaler()),
        ("clf", KNeighborsClassifier(n_neighbors=3))]),
    "Decision Tree": DecisionTreeClassifier(max_depth=4, random_state=42),
    "Random Forest": RandomForestClassifier(n_estimators=200, max_depth=5, random_state=42),
    "Gradient Boosting": GradientBoostingClassifier(n_estimators=150, max_depth=3, random_state=42),
}

rows = []
y_preds = {}
for name, model in models.items():
    preds = cross_val_predict(model, X, y, cv=cv)
    y_preds[name] = preds
    rows.append({
        "Model": name,
        "Accuracy": accuracy_score(y, preds),
        "Precision_macro": precision_score(y, preds, average="macro", zero_division=0),
        "Recall_macro": recall_score(y, preds, average="macro", zero_division=0),
        "F1_macro": f1_score(y, preds, average="macro", zero_division=0),
    })

comp = pd.DataFrame(rows).sort_values("F1_macro", ascending=False).reset_index(drop=True)
comp.to_csv(os.path.join(RES_DIR, "model_comparison.csv"), index=False)
print(comp.to_string(index=False))

# ---------- Hyperparameter tuning on best non-baseline ----------
best_name = comp[~comp["Model"].str.startswith("Baseline")].iloc[0]["Model"]
print(f"\nBest pre-tuning: {best_name}")

tune_grids = {
    "Random Forest": (RandomForestClassifier(random_state=42),
        {"n_estimators":[100,200,400], "max_depth":[3,5,7,None], "min_samples_split":[2,4]}),
    "Gradient Boosting": (GradientBoostingClassifier(random_state=42),
        {"n_estimators":[100,200], "max_depth":[2,3,4], "learning_rate":[0.05,0.1]}),
    "Logistic Regression": (Pipeline([("sc",StandardScaler()),("clf",LogisticRegression(max_iter=3000))]),
        {"clf__C":[0.1,1,5,10]}),
    "K-Nearest Neighbors": (Pipeline([("sc",StandardScaler()),("clf",KNeighborsClassifier())]),
        {"clf__n_neighbors":[1,3,5,7]}),
    "Decision Tree": (DecisionTreeClassifier(random_state=42),
        {"max_depth":[3,4,5,7,None],"min_samples_split":[2,4,6]}),
}

if best_name in tune_grids:
    base, grid = tune_grids[best_name]
    gs = GridSearchCV(base, grid, cv=cv, scoring="f1_macro", n_jobs=-1)
    gs.fit(X, y)
    best_model = gs.best_estimator_
    tune_info = {"model": best_name, "best_params": gs.best_params_,
                 "best_cv_f1_macro": float(gs.best_score_)}
    print("Tuned best params:", gs.best_params_, "CV F1:", gs.best_score_)
else:
    best_model = models[best_name]
    best_model.fit(X, y)
    tune_info = {"model": best_name, "best_params": "n/a",
                 "best_cv_f1_macro": float(comp.iloc[0]["F1_macro"])}

# Final fit on all data
best_model.fit(X, y)
joblib.dump({"model": best_model, "label_encoder": le, "features": FEATURES},
            os.path.join(MOD_DIR, "best_model.pkl"))

with open(os.path.join(RES_DIR, "tuning_summary.json"), "w") as f:
    json.dump(tune_info, f, indent=2, default=str)

# ---------- Confusion matrix of best (CV predictions) ----------
preds_best = cross_val_predict(best_model, X, y, cv=cv)
cm = confusion_matrix(y, preds_best, labels=range(len(class_names)))
fig, ax = plt.subplots(figsize=(6,5))
sns.heatmap(cm, annot=True, fmt="d", cmap="Blues",
            xticklabels=class_names, yticklabels=class_names, ax=ax)
ax.set_xlabel("Predicted"); ax.set_ylabel("Actual")
ax.set_title(f"Confusion Matrix — {best_name} (CV)")
fig.savefig(os.path.join(RES_DIR, "confusion_matrix.png"), dpi=130, bbox_inches="tight")
plt.close(fig)

# Classification report
report = classification_report(y, preds_best, target_names=class_names, zero_division=0)
with open(os.path.join(RES_DIR, "classification_report.txt"), "w") as f:
    f.write(f"Best model: {best_name}\n\n{report}\n")

# Comparison bar chart
fig, ax = plt.subplots(figsize=(10,5))
melt = comp.melt(id_vars="Model", value_vars=["Accuracy","F1_macro","Precision_macro","Recall_macro"],
                 var_name="Metric", value_name="Score")
sns.barplot(data=melt, x="Model", y="Score", hue="Metric", ax=ax)
ax.set_title("Model Comparison")
ax.set_ylim(0, 1.05)
ax.tick_params(axis="x", rotation=20)
fig.savefig(os.path.join(RES_DIR, "model_comparison.png"), dpi=130, bbox_inches="tight")
plt.close(fig)

# ---------- Feature importance ----------
fi = None
if hasattr(best_model, "feature_importances_"):
    fi = pd.Series(best_model.feature_importances_, index=FEATURES).sort_values(ascending=True)
elif hasattr(best_model, "named_steps") and hasattr(best_model.named_steps.get("clf", None), "coef_"):
    coefs = np.abs(best_model.named_steps["clf"].coef_).mean(axis=0)
    fi = pd.Series(coefs, index=FEATURES).sort_values(ascending=True)

if fi is not None:
    fig, ax = plt.subplots(figsize=(8,6))
    fi.plot.barh(ax=ax, color="seagreen")
    ax.set_title(f"Feature Importance — {best_name}")
    fig.savefig(os.path.join(RES_DIR, "feature_importance.png"), dpi=130, bbox_inches="tight")
    plt.close(fig)
    fi.sort_values(ascending=False).to_csv(os.path.join(RES_DIR, "feature_importance.csv"))

# ---------- Example predictions ----------
sample_idx = np.linspace(0, len(X)-1, num=min(5, len(X)), dtype=int)
ex_preds = best_model.predict(X[sample_idx])
ex_df = pd.DataFrame(X[sample_idx], columns=FEATURES)
ex_df.insert(0, "UserId", df["UserId"].iloc[sample_idx].values)
ex_df["Actual"] = le.inverse_transform(y[sample_idx])
ex_df["Predicted"] = le.inverse_transform(ex_preds)
ex_df.to_csv(os.path.join(RES_DIR, "example_predictions.csv"), index=False)

print("\nSaved best_model.pkl and all artefacts to results/")
