import subprocess
import sys
import time

def run_step(step_name, script_name):
    print(f"\n🟦 Running Step: {step_name}")
    print(f"📄 Executing: {script_name}")

    try:
        start = time.time()
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=True,
            text=True
        )
        end = time.time()

        print(f"⏳ Time Taken: {round(end - start, 2)}s")

        if result.returncode != 0:
            print(f"❌ ERROR running {script_name}")
            print(result.stderr)
            sys.exit(1)

        print(f"✅ Completed: {step_name}")
        print(result.stdout)

    except FileNotFoundError:
        print(f"❌ Could not find script: {script_name}")
        sys.exit(1)


def main():
    print("\n==============================")
    print("🚀 AIR QUALITY ETL PIPELINE")
    print("==============================\n")

    run_step("1️⃣ Extract", "extract.py")
    run_step("2️⃣ Transform", "transform.py")
    run_step("3️⃣ Load into Supabase", "load.py")
    run_step("4️⃣ Analysis & Reports", "etl_analysis.py")

    print("\n==============================")
    print("🎉 ETL Pipeline Finished Successfully!")
    print("==============================\n")


if __name__ == "__main__":
    main()