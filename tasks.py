"""Cross-platform task runner for common dev commands.

Usage:
  python -m tasks up
  python -m tasks down
  python -m tasks demo
  python -m tasks test
  python -m tasks lint
  python -m tasks type-check
  python -m tasks env
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess


def run(cmd: str, check: bool = True) -> int:
    print(f"$ {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, check=False, capture_output=True, text=True)
        if check and result.returncode != 0:
            # Print captured output for debugging
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print(result.stderr)
            # Detect common Docker issues
            if "pull access denied" in (result.stderr or ""):
                print("It looks like Docker failed to pull an image (pull access denied).")
                print("Possible fixes:")
                print(
                    "  - Run 'docker login' to authenticate with Docker Hub if the image requires it."
                )
                print("  - Pull the image manually: 'docker pull <image>' to see detailed error.")
                print(
                    "  - Override the image by setting REDPANDA_IMAGE in local/.env and re-run 'python -m tasks up'."
                )
                # Try to give more info by attempting a direct docker pull of the configured image
                try:
                    # read the configured image for redpanda from local/.env or default
                    image = "vectorized/redpanda:latest"
                    env_path = os.path.join(os.getcwd(), "local", ".env")
                    if os.path.exists(env_path):
                        with open(env_path, encoding="utf-8") as fh:
                            for line in fh:
                                if line.strip().startswith("REDPANDA_IMAGE="):
                                    image = line.strip().split("=", 1)[1]
                                    break
                    print(f"Attempting a direct 'docker pull {image}' to surface errors...")
                    run(f"docker pull {image}", check=False)
                except Exception:
                    pass
            # Detect common Docker credential helper problem
            if "docker-credential-desktop" in (result.stderr or ""):
                print(
                    "It looks like Docker's credential helper 'docker-credential-desktop' is not on PATH."
                )
                print(
                    "Ensure Docker Desktop is installed and that 'C:\\Program Files\\Docker\\Docker\\resources\\bin' is on your PATH."
                )
            raise subprocess.CalledProcessError(
                result.returncode, cmd, output=result.stdout, stderr=result.stderr
            )
        return result.returncode
    except FileNotFoundError as exc:  # command not found (e.g., docker missing)
        print(f"Command not found: {exc}. Ensure the required tool is installed and on your PATH.")
        raise


def _detect_compose() -> str | None:
    """Return the compose command to use, or None if not found.

    Prefers Docker's `docker compose` if `docker` exists, otherwise uses
    `podman compose` if `podman` exists. Returns None if neither is available.
    """
    docker_path = shutil.which("docker")
    if docker_path:
        return "docker compose"

    # On Windows Docker Desktop may not be on PATH; check common install locations
    possible = [
        r"C:\Program Files\Docker\Docker\resources\bin\docker.exe",
        r"C:\Program Files\Docker\Docker\resources\docker.exe",
        r"C:\Program Files\Docker\Docker\docker.exe",
    ]
    for p in possible:
        if os.path.exists(p):
            # Use the absolute path to docker executable
            return f'"{p}" compose'
    if shutil.which("podman"):
        return "podman compose"
    return None


def cmd_up() -> None:
    compose = _detect_compose()
    if not compose:
        print(
            "ERROR: Docker or Podman not found. Install Docker Desktop (https://www.docker.com/get-started) "
            "or Podman and ensure the CLI is on your PATH."
        )
        print("Verify by running: 'docker --version' or 'podman --version'.")
        raise SystemExit(1)
    run(f"{compose} -f local/docker-compose.yml up -d")


def cmd_down() -> None:
    compose = _detect_compose()
    if not compose:
        print(
            "ERROR: Docker or Podman not found. Install Docker Desktop or Podman and ensure the CLI is on your PATH."
        )
        raise SystemExit(1)
    run(f"{compose} -f local/docker-compose.yml down")


def cmd_demo() -> None:
    cmd_up()
    run("python services/event_generator/main.py --events 100 --dry-run")


def cmd_test() -> None:
    run("pytest -q")


def cmd_lint() -> None:
    run("ruff .")


def cmd_type_check() -> None:
    run("mypy .")


def cmd_env() -> None:
    # Create Conda environment in a cross-platform way
    if shutil.which("conda"):
        run("conda env create -f environment.yml -n rtod-env")
    else:
        print("'conda' not found on PATH. Please install Miniconda or Anaconda and run:")
        print("  conda env create -f environment.yml -n rtod-env")


def cmd_doctor() -> None:
    """Run quick environment checks and print actionable advice."""
    print("Checking Docker/Podman...")
    compose = _detect_compose()
    if compose:
        print(f"Compose command available: {compose}")
        try:
            if compose.startswith('"'):
                endq = compose.find('"', 1)
                exe = compose[1:endq]
            else:
                exe = compose.split()[0]
            run(f'"{exe}" --version', check=False)
            run(f'"{exe}" info', check=False)
        except Exception:
            print("Could not run docker/podman commands; ensure the daemon/service is running.")
    else:
        print("Docker/Podman not found on PATH or in common locations.")

    print("\nChecking MinIO client (mc)...")
    if shutil.which("mc"):
        run("mc --version", check=False)
    else:
        print(
            "'mc' (MinIO client) not found. Install from https://min.io/docs/minio/linux/reference/minio-mc.html"
        )

    print("\nChecking Conda...")
    if shutil.which("conda"):
        run("conda --version", check=False)
    else:
        print("'conda' not found. Install Miniconda or Anaconda and ensure it's on your PATH.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="tasks")
    parser.add_argument(
        "command", choices=["up", "down", "demo", "test", "lint", "type-check", "env", "doctor"]
    )  # noqa: E501
    args = parser.parse_args(argv)

    try:
        if args.command == "up":
            cmd_up()
        elif args.command == "down":
            cmd_down()
        elif args.command == "demo":
            cmd_demo()
        elif args.command == "test":
            cmd_test()
        elif args.command == "lint":
            cmd_lint()
        elif args.command == "type-check":
            cmd_type_check()
        elif args.command == "env":
            cmd_env()
        elif args.command == "doctor":
            cmd_doctor()
        return 0
    except subprocess.CalledProcessError as exc:
        print("Command failed.")
        return exc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
