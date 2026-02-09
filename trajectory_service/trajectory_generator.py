import math
import random
import uuid
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass

import numpy as np
import scipy.interpolate as si


@dataclass
class FourierCoefficient:
    amplitude: float
    frequency: float
    phase: float

@dataclass
class Trajectory:
    id: str
    points: List[Dict[str, float]]
    normalized_points: List[Dict[str, float]]
    parameters: Dict[str, Any]


def fourier_point(t: float, coefficients: List[FourierCoefficient]) -> Tuple[float, float]:
    x, y = 0.0, 0.0
    for c in coefficients:
        x += c.amplitude * math.cos(c.frequency * t + c.phase)
        y += c.amplitude * math.sin(c.frequency * t + c.phase)
    return x, y

def generate_random_coefficients(n: int = None) -> List[FourierCoefficient]:
    if n is None:
        n = random.randint(3, 8)

    coeffs = []
    for i in range(n):
        coeffs.append(
            FourierCoefficient(
                amplitude=random.uniform(0.5, 2.0) / (i + 1),
                frequency=i + 1,
                phase=random.uniform(0, 2 * math.pi)
            )
        )
    return coeffs

def generate_fourier_points(num_points: int) -> Tuple[List[Tuple[float, float]], Dict]:
    time_range = random.uniform(4 * math.pi, 8 * math.pi)
    coeffs = generate_random_coefficients()

    points = []
    for i in range(num_points):
        t = (i / num_points) * time_range
        points.append(fourier_point(t, coeffs))

    params = {
        "trajectory_type": "fourier",
        "num_points": num_points,
        "time_range": round(time_range, 4),
        "num_coefficients": len(coeffs),
    }

    return points, params


def random_spline(steps: int = 600, n_ctrl: int = 8) -> Tuple[np.ndarray, np.ndarray]:
    ctrl = np.random.uniform(-5, 5, (n_ctrl, 2))
    tck, _ = si.splprep(ctrl.T, s=0)
    u_fine = np.linspace(0, 1, steps)
    x, y = si.splev(u_fine, tck)
    return x, y

def generate_spline_points(num_points: int) -> Tuple[List[Tuple[float, float]], Dict]:
    steps = max(num_points, 200)
    n_ctrl = random.randint(6, 10)

    x, y = random_spline(steps=steps, n_ctrl=n_ctrl)
    points = list(zip(x, y))

    params = {
        "trajectory_type": "random_spline",
        "num_points": len(points),
        "control_points": n_ctrl
    }

    return points, params


def normalize_to_unit_coordinates(points: List[Tuple[float, float]]) -> List[Dict[str, float]]:
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]

    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)

    norm = []
    for x, y in points:
        nx = (x - min_x) / (max_x - min_x) if max_x != min_x else 0.5
        ny = (y - min_y) / (max_y - min_y) if max_y != min_y else 0.5
        norm.append({"x": round(nx, 4), "y": round(ny, 4)})

    return norm


def generate_unit_trajectory() -> Trajectory:
    num_points = random.randint(100, 300)

    generator = random.choice(["fourier", "spline"])

    if generator == "fourier":
        raw_points, params = generate_fourier_points(num_points)
    else:
        raw_points, params = generate_spline_points(num_points)

    normalized = normalize_to_unit_coordinates(raw_points)

    trajectory_id = str(uuid.uuid4())[:8]
    params["coordinates"] = "normalized_0_1"

    return Trajectory(
        id=trajectory_id,
        points=[],
        normalized_points=normalized,
        parameters=params
    )
