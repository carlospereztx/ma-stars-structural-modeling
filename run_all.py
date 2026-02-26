import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "src"

PIPELINE = [
    "01_stage_stars.py",
    "02_check_star_summary.py",
    "03_stage_enrollment.py",
    "04_inspect_enrollment.py",
    "05_build_weights.py",
    "06_stage_saipe_poverty_api.py",
    "07_build_poverty_exposure.py",
    "08_build_near_threshold_analysis.py",
    "09_threshold_stats.py",
    "10_stage_rural_rucc.py",
    "11_build_rural_exposure.py",
    "12_build_threshold_with_rural.py",
    "13_threshold_logit_structural.py",
    "14_model_full_stars.py",
    "15_stage_hpsa.py",
    "16_build_hpsa_exposure.py",
    "17_export_report_artifacts.py",
    "18_make_report_figures.py",
]


def run_script(script_name):
    script_path = SRC_DIR / script_name
    print(f"\n==> Running {script_name}")
    result = subprocess.run([sys.executable, str(script_path)], cwd=str(BASE_DIR))
    if result.returncode != 0:
        sys.exit(result.returncode)


def main():
    print("MA Stars Pipeline")
    print("=================")

    for script in PIPELINE:
        run_script(script)

    print("\nPipeline complete.")


if __name__ == "__main__":
    main()