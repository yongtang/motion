import argparse
import json
import logging

import pxr.Usd
import pxr.UsdPhysics

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def f_target(stage, articulation, joint):
    parent = joint.GetBody0Rel().GetTargets()
    child = joint.GetBody1Rel().GetTargets()
    assert len(parent) == 1 or (
        len(parent) == 0 and str(articulation.GetPath()) == str(joint.GetPath())
    ), f"{joint} - {parent}"
    parent = next(iter(parent)) if len(parent) == 1 else None
    assert len(child) == 1, f"{joint} - {child}"
    child = next(iter(child))
    return parent, child


def f_origin(stage, articulation, joint):
    def f_rpy(quaternion):
        # Gf.Quatf stores (real, imag)
        rotation = pxr.Gf.Rotation(quaternion)
        hpr = rotation.Decompose(
            pxr.Gf.Vec3d(0, 0, 1), pxr.Gf.Vec3d(0, 1, 0), pxr.Gf.Vec3d(1, 0, 0)
        )
        # USD "HPR" is heading(z), pitch(y), roll(x).
        # URDF wants roll(x), pitch(y), yaw(z).
        # Map: roll = R, pitch = P, yaw = H
        r = float(hpr[2]) * 3.141592653589793 / 180.0
        p = float(hpr[1]) * 3.141592653589793 / 180.0
        y = float(hpr[0]) * 3.141592653589793 / 180.0
        return [r, p, y]

    xyz = joint.GetLocalPos1Attr().Get()
    rpy = f_rpy(joint.GetLocalRot1Attr().Get())
    origin = {
        "xyz": " ".join(map(lambda e: f"{e}", xyz)),
        "rpy": " ".join(map(lambda e: f"{e}", rpy)),
    }
    return origin


def f_limit(stage, articulation, joint):
    lower = joint.GetLowerLimitAttr().Get()
    upper = joint.GetUpperLimitAttr().Get()
    velocity = 1.0
    effort = 100
    limit = {
        "lower": lower,
        "upper": upper,
        **({"velocity": velocity} if velocity is not None else {}),
        **({"effort": effort} if effort is not None else {}),
    }
    return limit


def f_axis(stage, articulation, joint):
    axis = joint.GetAxisAttr().Get()
    axis = axis.upper()
    mapping = {
        "X": "1 0 0",
        "Y": "0 1 0",
        "Z": "0 0 1",
    }
    axis = mapping.get(axis)
    return axis


def f_any(stage, articulation, entry):
    assert pxr.UsdPhysics.Joint(entry), entry
    joint = pxr.UsdPhysics.Joint(entry)
    parent, child = f_target(stage, articulation, joint)
    origin = f_origin(stage, articulation, joint)
    return {
        "name": str(entry.GetPath()),
        "type": "fixed",
        **({"parent": parent} if parent is not None else {}),
        **({"child": child} if child is not None else {}),
    }


def f_fixed(stage, articulation, entry):
    assert pxr.UsdPhysics.FixedJoint(entry), entry
    joint = pxr.UsdPhysics.FixedJoint(entry)
    parent, child = f_target(stage, articulation, joint)
    origin = f_origin(stage, articulation, joint)
    return {
        "name": str(entry.GetPath()),
        "type": "fixed",
        **({"parent": parent} if parent is not None else {}),
        **({"child": child} if child is not None else {}),
    }


def f_revolute(stage, articulation, entry):
    assert pxr.UsdPhysics.RevoluteJoint(entry), entry
    joint = pxr.UsdPhysics.RevoluteJoint(entry)
    parent, child = f_target(stage, articulation, joint)
    origin = f_origin(stage, articulation, joint)
    limit = f_limit(stage, articulation, joint)
    axis = f_axis(stage, articulation, joint)
    return {
        "name": str(entry.GetPath()),
        "type": ("continuous" if limit is None else "revolute"),
        **({"parent": parent} if parent is not None else {}),
        **({"child": child} if child is not None else {}),
        "limit": limit,
        "axis": axis,
    }


def f_prismatic(stage, articulation, entry):
    assert pxr.UsdPhysics.PrismaticJoint(entry), entry
    joint = pxr.UsdPhysics.PrismaticJoint(entry)
    parent, child = f_target(stage, articulation, joint)
    origin = f_origin(stage, articulation, joint)
    limit = f_limit(stage, articulation, joint)
    axis = f_axis(stage, articulation, joint)
    return {
        "name": str(entry.GetPath()),
        "type": "prismatic",
        **({"parent": parent} if parent is not None else {}),
        **({"child": child} if child is not None else {}),
        "limit": limit,
        "axis": axis,
    }


def f_joint(stage, articulation, entry):
    entries = {
        pxr.UsdPhysics.FixedJoint: f_fixed,
        pxr.UsdPhysics.RevoluteJoint: f_revolute,
        pxr.UsdPhysics.PrismaticJoint: f_prismatic,
    }
    joint = stage.GetPrimAtPath(entry)
    joint = next(
        (g(stage, articulation, joint) for f, g in entries.items() if f(joint)),
        f_any(stage, articulation, joint),
    )

    assert joint is not None, f"{joint} vs. {entry}"
    return joint


def f_articulation(stage, articulation):
    # Limit to valid parent and child - articulation included to prune link.
    # Articulation not in final joint as it is not base link
    candidate = [
        e
        for e in stage.Traverse()
        if (
            (
                e.GetRelationship("physics:body0")
                and e.GetRelationship("physics:body0").IsValid()
                and len(pxr.UsdPhysics.Joint(e).GetBody0Rel().GetTargets()) > 0
            )
            and (
                e.GetRelationship("physics:body1")
                and e.GetRelationship("physics:body1").IsValid()
                and len(pxr.UsdPhysics.Joint(e).GetBody1Rel().GetTargets()) > 0
            )
        )
    ] + [articulation]

    link, joint = set(), set()
    count_link, count_joint = len(link), len(joint)
    if str(articulation.GetPath()) in [str(e.GetPath()) for e in candidate]:
        link |= {
            str(e)
            for e in pxr.UsdPhysics.Joint(articulation).GetBody1Rel().GetTargets()
        }
    else:
        link |= {str(articulation.GetPath())}

    while count_link < len(link) or count_joint < len(joint):
        count_link, count_joint = len(link), len(joint)
        for e in link:
            for i in candidate:
                if e in [
                    str(e) for e in pxr.UsdPhysics.Joint(i).GetBody0Rel().GetTargets()
                ]:
                    joint |= {str(i.GetPath())}
        for i in joint:
            link |= {
                str(e)
                for e in pxr.UsdPhysics.Joint(stage.GetPrimAtPath(i))
                .GetBody1Rel()
                .GetTargets()
            }

    link = list(link)

    joint = list(f_joint(stage, articulation, e) for e in joint)

    return {
        "link": link,
        "joint": joint,
    }


def f_urdf(articulation, link, joint):
    line = []
    line.append(f'<robot name="robot">')

    for e in link:
        line.append(f'  <link name="{e}" />')

    for e in joint:
        line.append(f"  <joint name=\"{e['name']}\" type=\"{e['type']}\">")
        if "parent" in e:
            line.append(f"    <parent link=\"{e['parent']}\"/>")
        if "child" in e:
            line.append(f"    <child link=\"{e['child']}\"/>")
        if "limit" in e:
            item = ""
            item += f"    <limit lower=\"{e['limit']['lower']}\" upper=\"{e['limit']['upper']}\""
            if "velocity" in e["limit"]:
                item += f" velocity=\"{e['limit']['velocity']}\""
            if "effort" in e["limit"]:
                item += f" effort=\"{e['limit']['effort']}\""
            item += f"/>"
            line.append(item)
        if "axis" in e:
            line.append(f"    <axis xyz=\"{e['axis']}\"/>")
        line.append(f"  </joint>")

    line.append(f"</robot>")

    return "\n".join(line)


def f_desc(articulation, link, joint):
    line = []
    line.append(f"robot_name: robot")
    line.append(f"cspace:")

    entries = list(e for e in joint if e["type"] != "fixed")
    for e in entries:
        line.append(f"  - {e['name']}")

    entries = list("0" for e in entries)
    line.append(f"default_q: [{', '.join(entries)}]")

    return "\n".join(line)


def urdf(file):
    stage = pxr.Usd.Stage.Open(file)
    assert stage, f"Stage: {file}"

    articulation = {
        str(e.GetPath()): f_articulation(stage, e)
        for e in stage.Traverse()
        if e.HasAPI(pxr.UsdPhysics.ArticulationRootAPI)
    }

    log.info(f"[urdf] Articulation: {articulation}")

    log.info(f"[urdf] URDF:")

    data = {
        e: {
            "urdf": f_urdf(e, entry["link"], entry["joint"]),
            "desc": f_desc(e, entry["link"], entry["joint"]),
        }
        for e, entry in articulation.items()
    }
    for e, entry in data.items():
        log.info(f"{e}:\n{entry['urdf']}\n{entry['desc']}\n{'-'*20}")

    return data


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)

    args = parser.parse_args()

    log.info(f"[main] Args: {json.dumps(vars(args), sort_keys=True)}")

    urdf(args.file)


if __name__ == "__main__":
    main()
