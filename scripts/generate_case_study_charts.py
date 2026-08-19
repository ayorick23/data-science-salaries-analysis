"""Genera los gráficos estáticos usados en docs/CASE_STUDY.md a partir de
data/processed/compensation.csv. Requiere haber corrido `uv run python src/etl.py`
al menos una vez. Uso: `uv run python scripts/generate_case_study_charts.py`.
"""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = REPO_ROOT / "data" / "processed" / "compensation.csv"
OUT_DIR = REPO_ROOT / "docs" / "assets" / "case-study"

# --- tokens ---
SURFACE = "#fcfcfb"
INK_PRIMARY = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"
BLUE = "#2a78d6"  # series-1
ORANGE = "#eb6834"  # series-2

plt.rcParams.update(
    {
        "font.family": "sans-serif",
        "font.sans-serif": ["Segoe UI", "Arial", "DejaVu Sans"],
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "text.color": INK_PRIMARY,
        "axes.edgecolor": BASELINE,
        "axes.labelcolor": INK_SECONDARY,
        "xtick.color": INK_MUTED,
        "ytick.color": INK_MUTED,
        "axes.titlesize": 13,
        "axes.titleweight": "bold",
        "font.size": 10.5,
    }
)


def style_ax(ax, grid_axis="y"):
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    for spine in ["left", "bottom"]:
        ax.spines[spine].set_color(BASELINE)
        ax.spines[spine].set_linewidth(1)
    ax.grid(axis=grid_axis, color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)


def usd_thousands(x, _pos):
    return f"${x / 1000:,.0f}k"


def plot_salary_distribution(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(9, 4.6), dpi=200)
    salary = df["salary_usd"]
    ax.hist(salary, bins=60, color=BLUE, edgecolor=SURFACE, linewidth=0.4, zorder=2)
    median, mean = salary.median(), salary.mean()
    ax.axvline(median, color=INK_PRIMARY, linewidth=1.4, linestyle="--", zorder=3)
    ax.axvline(mean, color=ORANGE, linewidth=1.4, linestyle="--", zorder=3)
    ylim = ax.get_ylim()[1]
    ax.text(
        median,
        ylim * 0.97,
        f"  Mediana: ${median:,.0f}",
        color=INK_PRIMARY,
        va="top",
        ha="left",
        fontsize=9.5,
        fontweight="bold",
    )
    ax.text(
        mean,
        ylim * 0.86,
        f"  Media: ${mean:,.0f}",
        color=ORANGE,
        va="top",
        ha="left",
        fontsize=9.5,
        fontweight="bold",
    )
    ax.set_xlim(0, 400000)
    ax.xaxis.set_major_formatter(usd_thousands)
    ax.set_xlabel("Salario anual (USD)")
    ax.set_ylabel("Cantidad de filas")
    ax.set_title(f"Distribución de salarios — sesgo a la derecha (skew ≈ {salary.skew():.2f})", loc="left")
    style_ax(ax)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "salary-distribution.png")
    plt.close(fig)


def plot_salary_by_experience(df: pd.DataFrame) -> None:
    order = ["Junior", "Mid", "Senior", "Executive"]
    fig, ax = plt.subplots(figsize=(7.5, 4.8), dpi=200)
    data = [df.loc[df["experience_level"] == lvl, "salary_usd"].values for lvl in order]
    ax.boxplot(
        data,
        positions=range(len(order)),
        widths=0.5,
        patch_artist=True,
        showfliers=True,
        flierprops=dict(
            marker="o", markersize=2.5, markerfacecolor=INK_MUTED, markeredgecolor="none", alpha=0.35
        ),
        medianprops=dict(color=INK_PRIMARY, linewidth=1.8),
        boxprops=dict(facecolor=BLUE, edgecolor=BLUE, alpha=0.85),
        whiskerprops=dict(color=BASELINE, linewidth=1.2),
        capprops=dict(color=BASELINE, linewidth=1.2),
    )
    for i, lvl in enumerate(order):
        med = df.loc[df["experience_level"] == lvl, "salary_usd"].median()
        n = (df["experience_level"] == lvl).sum()
        ax.text(
            i + 0.28,
            med + 9000,
            f"${med:,.0f}",
            va="bottom",
            ha="center",
            fontsize=9.5,
            fontweight="bold",
            color=INK_PRIMARY,
        )
        ax.text(
            i,
            -0.09,
            f"n={n:,}",
            transform=ax.get_xaxis_transform(),
            ha="center",
            va="top",
            fontsize=8.5,
            color=INK_MUTED,
        )
    ax.set_xticks(range(len(order)))
    ax.set_xticklabels(order)
    ax.set_ylim(0, 500000)
    ax.yaxis.set_major_formatter(usd_thousands)
    ax.set_ylabel("Salario anual (USD)")
    ax.set_title("Salario por nivel de experiencia — Junior → Senior casi duplica la mediana", loc="left")
    style_ax(ax)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "salary-by-experience.png")
    plt.close(fig)


def plot_salary_by_role(df: pd.DataFrame) -> None:
    medians = df.groupby("job_category")["salary_usd"].median().sort_values(ascending=True)
    roles = medians.index.tolist()
    top_role = medians.idxmax()

    fig, ax = plt.subplots(figsize=(9, 5.2), dpi=200)
    data = [df.loc[df["job_category"] == r, "salary_usd"].values for r in roles]
    colors = [ORANGE if r == top_role else BLUE for r in roles]
    bp = ax.boxplot(
        data,
        positions=range(len(roles)),
        vert=False,
        widths=0.55,
        patch_artist=True,
        showfliers=False,
        medianprops=dict(color=INK_PRIMARY, linewidth=1.8),
        whiskerprops=dict(color=BASELINE, linewidth=1.2),
        capprops=dict(color=BASELINE, linewidth=1.2),
    )
    for patch, c in zip(bp["boxes"], colors, strict=True):
        patch.set_facecolor(c)
        patch.set_edgecolor(c)
        patch.set_alpha(0.85)
    for i, r in enumerate(roles):
        med = medians[r]
        ax.text(
            med,
            i,
            f"  ${med:,.0f}",
            va="center",
            ha="left",
            fontsize=9,
            fontweight="bold" if r == top_role else "normal",
            color=INK_PRIMARY,
        )
    ax.set_yticks(range(len(roles)))
    ax.set_yticklabels(roles)
    ax.set_xlim(0, 420000)
    ax.xaxis.set_major_formatter(usd_thousands)
    ax.set_xlabel("Salario anual (USD)")
    ax.set_title("Salario mediano por familia de rol — ML/AI Engineer a la cabeza", loc="left")
    style_ax(ax, grid_axis="x")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "salary-by-role.png")
    plt.close(fig)


def plot_country_confidence_intervals(df: pd.DataFrame) -> None:
    rng = np.random.default_rng(42)
    counts = df["company_location"].value_counts()
    eligible = counts[counts >= 5].index

    rows = []
    for pais in eligible:
        vals = df.loc[df["company_location"] == pais, "salary_usd"].values
        n = len(vals)
        resamples = rng.integers(0, n, size=(1000, n))
        boot_means = vals[resamples].mean(axis=1)
        lo, hi = np.percentile(boot_means, [2.5, 97.5])
        rows.append((pais, n, vals.mean(), lo, hi))

    ci_df = pd.DataFrame(rows, columns=["pais", "n", "promedio", "lo", "hi"]).sort_values(
        "promedio", ascending=False
    )
    top15 = ci_df.head(15).sort_values("promedio")

    fig, ax = plt.subplots(figsize=(9, 6), dpi=200)
    y = np.arange(len(top15))
    xerr = np.vstack([top15["promedio"] - top15["lo"], top15["hi"] - top15["promedio"]])
    colors = [ORANGE if n < 30 else BLUE for n in top15["n"]]
    ax.errorbar(
        top15["promedio"],
        y,
        xerr=xerr,
        fmt="none",
        ecolor=BASELINE,
        elinewidth=1.6,
        capsize=3,
        capthick=1.6,
        zorder=2,
    )
    ax.scatter(top15["promedio"], y, color=colors, s=46, zorder=3, edgecolor=SURFACE, linewidth=0.6)
    for yi, (_pais, n, prom) in enumerate(zip(top15["pais"], top15["n"], top15["promedio"], strict=True)):
        ax.text(prom, yi + 0.32, f"n={n:,}", ha="center", va="bottom", fontsize=7.8, color=INK_MUTED)
    ax.set_yticks(y)
    ax.set_yticklabels(top15["pais"])
    ax.xaxis.set_major_formatter(usd_thousands)
    ax.set_xlabel("Salario promedio (USD), IC 95% por bootstrap (1,000 remuestreos)")
    ax.set_title("Top países por salario promedio — el intervalo se dispara cuando n es chico", loc="left")
    handles = [
        plt.Line2D([0], [0], marker="o", color="none", markerfacecolor=BLUE, markersize=7, label="n ≥ 30"),
        plt.Line2D(
            [0],
            [0],
            marker="o",
            color="none",
            markerfacecolor=ORANGE,
            markersize=7,
            label="n < 30 (leer con cautela)",
        ),
    ]
    ax.legend(handles=handles, loc="lower right", frameon=False, fontsize=9, labelcolor=INK_SECONDARY)
    style_ax(ax, grid_axis="x")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "country-confidence-intervals.png")
    plt.close(fig)


def plot_ppp_adjustment(df: pd.DataFrame) -> None:
    countries = ["US", "CH", "DE", "GB", "IN", "BR"]
    labels = {
        "US": "Estados Unidos",
        "CH": "Suiza",
        "DE": "Alemania",
        "GB": "Reino Unido",
        "IN": "India",
        "BR": "Brasil",
    }
    sub = (
        df[df["company_location"].isin(countries)]
        .groupby("company_location")
        .agg(nominal=("salary_usd", "mean"), ppp=("salary_usd_ppp", "mean"), n=("salary_usd", "size"))
    )
    sub = sub.loc[countries]

    fig, ax = plt.subplots(figsize=(9, 5), dpi=200)
    x = np.arange(len(countries))
    w = 0.34
    bars = [
        ax.bar(x - w / 2, sub["nominal"], width=w, color=BLUE, label="Nominal (USD)", zorder=2),
        ax.bar(x + w / 2, sub["ppp"], width=w, color=ORANGE, label="Ajustado por PPP", zorder=2),
    ]
    for rects in bars:
        for rect in rects:
            h = rect.get_height()
            ax.text(
                rect.get_x() + rect.get_width() / 2,
                h,
                f"${h / 1000:,.0f}k",
                ha="center",
                va="bottom",
                fontsize=8.3,
                color=INK_SECONDARY,
            )
    ax.set_xticks(x)
    ax.set_xticklabels([labels[c] for c in countries])
    ax.yaxis.set_major_formatter(usd_thousands)
    ax.set_ylabel("Salario promedio (USD)")
    ax.set_title(
        "Nominal vs. ajustado por poder adquisitivo — India supera a Suiza en términos reales", loc="left"
    )
    ax.legend(loc="upper right", frameon=False, fontsize=9.5, labelcolor=INK_SECONDARY)
    style_ax(ax)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "ppp-adjustment-comparison.png")
    plt.close(fig)


def main() -> None:
    if not DATA_PATH.exists():
        raise SystemExit(f"No se encontró {DATA_PATH}. Corré primero: uv run python src/etl.py")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(DATA_PATH, index_col=0)

    plot_salary_distribution(df)
    plot_salary_by_experience(df)
    plot_salary_by_role(df)
    plot_country_confidence_intervals(df)
    plot_ppp_adjustment(df)

    print(f"Gráficos generados en {OUT_DIR}")


if __name__ == "__main__":
    main()
