import argparse
import contextlib
import logging
import pathlib
import tempfile

import pxr
import pxr.Sdf
import pxr.Usd
import pxr.UsdGeom
import yaml

log = logging.getLogger(__name__)


@contextlib.contextmanager
def f_stage(omniverse):
    with tempfile.TemporaryDirectory() as directory:
        stage = pxr.Usd.Stage.CreateNew(
            str(pathlib.Path(directory).joinpath("scene.usd"))
        )
        if omniverse:
            from omni.isaac.kit import SimulationApp

            simulation_app = SimulationApp({"headless": True})
            try:
                yield stage
            finally:
                simulation_app.close()
        else:
            yield stage


def main():
    parser = argparse.ArgumentParser(description="USD")

    parser.add_argument("--file", type=str, required=True, help="Path to USD assembly")

    parser.add_argument(
        "--config", type=str, required=True, help="Path to configuration"
    )
    parser.add_argument(
        "--log-level",
        type=str.lower,
        default="info",
        choices=["debug", "info", "warning", "error", "critical"],
        help="Logging level (default: info)",
    )

    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    with open(args.config) as f:
        config = yaml.safe_load(f.read())
        omniverse = any(
            e["path"].startswith("omniverse://")
            for e in (config.get("robot", []) + config.get("background", []))
        )

    with f_stage(omniverse=omniverse) as stage:
        # Create stage with a /World root
        world = pxr.UsdGeom.Xform.Define(stage, pxr.Sdf.Path("/World")).GetPrim()
        stage.SetDefaultPrim(world)

        for background in config.get("background", []):
            prim = stage.DefinePrim(pxr.Sdf.Path(background["prim"]))
            prim.GetReferences().AddReference(assetPath=background["path"])
        for robot in config.get("robot", []):
            prim = stage.DefinePrim(pxr.Sdf.Path(robot["prim"]))
            prim.GetReferences().AddReference(assetPath=robot["path"])
        # Assembly to single USD file
        stage.Export(args.file, args={"flatten": "true"})


if __name__ == "__main__":
    main()
