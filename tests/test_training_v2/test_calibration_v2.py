"""
Unit tests for V2 Probability Calibration Framework.
"""

import numpy as np
import pytest

from src.ml.training.calibration import (
    PlattSigmoidCalibrator,
    TemperatureScalingCalibrator,
    UncalibratedWrapper,
    get_calibrator,
    CalibrationMethod,
)


def test_platt_sigmoid_calibrator():
    """Verifies Platt Sigmoid calibration fits and outputs valid normalized probabilities."""
    rng = np.random.default_rng(42)
    n = 200

    # Synthetic uncalibrated overconfident probabilities
    raw_probs = rng.dirichlet((0.5, 0.5, 0.5), size=n)
    y_true = rng.integers(0, 3, size=n)

    calibrator = PlattSigmoidCalibrator()
    calibrator.fit(raw_probs, y_true)

    # Test calibration on new sample
    test_probs = rng.dirichlet((0.5, 0.5, 0.5), size=20)
    calib_probs = calibrator.calibrate(test_probs)

    assert calib_probs.shape == (20, 3)
    # Must be valid probabilities: [0, 1] and sum to 1
    assert (calib_probs >= 0.0).all()
    assert (calib_probs <= 1.0).all()
    np.testing.assert_allclose(calib_probs.sum(axis=1), np.ones(20), atol=1e-5)

    params = calibrator.get_params()
    assert params["method"] == CalibrationMethod.PLATT_SIGMOID.value
    assert params["fitted"] is True


def test_temperature_scaling_calibrator():
    """Verifies Temperature Scaling calibrator fits and outputs valid probabilities."""
    rng = np.random.default_rng(42)
    n = 200

    raw_probs = rng.dirichlet((0.3, 0.3, 0.3), size=n)
    y_true = rng.integers(0, 3, size=n)

    calibrator = TemperatureScalingCalibrator()
    calibrator.fit(raw_probs, y_true)

    assert calibrator.temperature > 0.0

    test_probs = rng.dirichlet((0.3, 0.3, 0.3), size=15)
    calib_probs = calibrator.calibrate(test_probs)

    assert calib_probs.shape == (15, 3)
    assert (calib_probs >= 0.0).all()
    assert (calib_probs <= 1.0).all()
    np.testing.assert_allclose(calib_probs.sum(axis=1), np.ones(15), atol=1e-5)

    # Temperature scaling must strictly preserve the argmax ranking of the classes
    assert (np.argmax(calib_probs, axis=1) == np.argmax(test_probs, axis=1)).all()


def test_uncalibrated_wrapper():
    """Verifies UncalibratedWrapper pass-through behavior."""
    raw_probs = np.array([[0.7, 0.2, 0.1], [0.1, 0.8, 0.1]])
    calibrator = UncalibratedWrapper()
    out = calibrator.calibrate(raw_probs)
    np.testing.assert_allclose(raw_probs, out)


def test_factory_method():
    """Verifies get_calibrator factory."""
    assert isinstance(get_calibrator("none"), UncalibratedWrapper)
    assert isinstance(get_calibrator("platt_sigmoid"), PlattSigmoidCalibrator)
    assert isinstance(get_calibrator("temperature_scaling"), TemperatureScalingCalibrator)
