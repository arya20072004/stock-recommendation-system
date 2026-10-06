"""
src.ml.training.calibration
Post-Hoc Probability Calibration for V2 Architecture.
Supports One-vs-Rest Platt Sigmoid Scaling and Multiclass Temperature Scaling.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Union, Dict, Any
import numpy as np
from scipy.optimize import minimize
from sklearn.calibration import CalibratedClassifierCV


class CalibrationMethod(str, Enum):
    NONE = "none"
    PLATT_SIGMOID = "platt_sigmoid"
    TEMPERATURE_SCALING = "temperature_scaling"


class MulticlassCalibrator(ABC):
    """Abstract base class for V2 multiclass probability calibrators."""

    @abstractmethod
    def fit(self, probs_or_logits: np.ndarray, y_true: np.ndarray) -> "MulticlassCalibrator":
        pass

    @abstractmethod
    def calibrate(self, probs_or_logits: np.ndarray) -> np.ndarray:
        pass

    @abstractmethod
    def get_params(self) -> Dict[str, Any]:
        pass


class UncalibratedWrapper(MulticlassCalibrator):
    """Pass-through identity calibrator for baseline comparisons."""

    def fit(self, probs_or_logits: np.ndarray, y_true: np.ndarray) -> "UncalibratedWrapper":
        return self

    def calibrate(self, probs_or_logits: np.ndarray) -> np.ndarray:
        p = np.asarray(probs_or_logits, dtype=float)
        sums = p.sum(axis=1, keepdims=True)
        sums = np.where(sums <= 0, 1.0, sums)
        return p / sums

    def get_params(self) -> Dict[str, Any]:
        return {"method": CalibrationMethod.NONE.value}


class PlattSigmoidCalibrator(MulticlassCalibrator):
    """
    One-vs-Rest Platt Sigmoid Scaling with Vector Normalization.
    Fits independent binary logistic calibrations for each class,
    then normalizes the output vector so that sum(p) == 1.
    """

    def __init__(self, base_estimator: Optional[Any] = None):
        self.base_estimator = base_estimator
        self.calibrated_classifier: Optional[CalibratedClassifierCV] = None
        self._fitted = False

    def fit_with_estimator(self, X_calib: np.ndarray, y_calib: np.ndarray, base_estimator: Any) -> "PlattSigmoidCalibrator":
        """Fits using prefit scikit-learn compatible base estimator."""
        self.base_estimator = base_estimator
        self.calibrated_classifier = CalibratedClassifierCV(
            estimator=base_estimator,
            method="sigmoid",
            cv="prefit"
        )
        self.calibrated_classifier.fit(X_calib, y_calib)
        self._fitted = True
        return self

    def fit(self, probs_or_logits: np.ndarray, y_true: np.ndarray) -> "PlattSigmoidCalibrator":
        """
        Fits independent Platt sigmoids directly on raw probability predictions:
        P_calib_k = sigma(A_k * logit_k + B_k)
        """
        probs = np.asarray(probs_or_logits, dtype=float)
        probs = np.clip(probs, 1e-7, 1.0 - 1e-7)
        # Convert to log-odds
        logits = np.log(probs / (1.0 - probs))

        n_samples, n_classes = logits.shape
        self.params_ = []

        for k in range(n_classes):
            binary_y = (y_true == k).astype(float)
            x_k = logits[:, k]

            # Minimize binary cross entropy: loss(A, B)
            def _neg_log_likelihood(params):
                a, b = params
                p = 1.0 / (1.0 + np.exp(-(a * x_k + b)))
                p = np.clip(p, 1e-12, 1.0 - 1e-12)
                return -np.sum(binary_y * np.log(p) + (1.0 - binary_y) * np.log(1.0 - p))

            res = minimize(_neg_log_likelihood, [1.0, 0.0], method="L-BFGS-B")
            self.params_.append((float(res.x[0]), float(res.x[1])))

        self._fitted = True
        return self

    def calibrate(self, probs_or_logits: np.ndarray) -> np.ndarray:
        if not self._fitted:
            raise RuntimeError("Calibrator has not been fitted.")

        if self.calibrated_classifier is not None:
            # Calibrate features directly if wrapper was used
            return self.calibrated_classifier.predict_proba(probs_or_logits)

        probs = np.asarray(probs_or_logits, dtype=float)
        probs = np.clip(probs, 1e-7, 1.0 - 1e-7)
        logits = np.log(probs / (1.0 - probs))

        n_samples, n_classes = logits.shape
        calibrated = np.zeros_like(logits)

        for k in range(n_classes):
            a, b = self.params_[k]
            calibrated[:, k] = 1.0 / (1.0 + np.exp(-(a * logits[:, k] + b)))

        # Vector normalization
        row_sums = calibrated.sum(axis=1, keepdims=True)
        row_sums = np.where(row_sums <= 0, 1.0, row_sums)
        return calibrated / row_sums

    def get_params(self) -> Dict[str, Any]:
        return {
            "method": CalibrationMethod.PLATT_SIGMOID.value,
            "fitted": self._fitted,
            "coefficients": getattr(self, "params_", None),
        }


class TemperatureScalingCalibrator(MulticlassCalibrator):
    """
    Multiclass Temperature Scaling.
    Optimizes a single scalar temperature T > 0 to soften/sharpen logits:
      p_k = exp(z_k / T) / sum_j exp(z_j / T)
    Preserves exact multiclass ranking while dramatically improving calibration.
    """

    def __init__(self, eps: float = 1e-7):
        self.eps = eps
        self.temperature: float = 1.0
        self._fitted = False

    def _probs_to_logits(self, probs: np.ndarray) -> np.ndarray:
        p = np.clip(probs, self.eps, 1.0 - self.eps)
        log_p = np.log(p)
        # Center logits so mean is 0 per row (softmax invariant)
        return log_p - log_p.mean(axis=1, keepdims=True)

    def fit(self, probs_or_logits: np.ndarray, y_true: np.ndarray) -> "TemperatureScalingCalibrator":
        probs = np.asarray(probs_or_logits, dtype=float)
        logits = self._probs_to_logits(probs)
        y = np.asarray(y_true, dtype=int)
        n_samples = len(y)

        def _nll(temp_arr):
            temp = max(temp_arr[0], 0.01)
            scaled = logits / temp
            exp_scaled = np.exp(scaled - scaled.max(axis=1, keepdims=True))
            softmax_probs = exp_scaled / exp_scaled.sum(axis=1, keepdims=True)
            softmax_probs = np.clip(softmax_probs, self.eps, 1.0 - self.eps)
            loss = -np.sum(np.log(softmax_probs[np.arange(n_samples), y]))
            return loss

        res = minimize(_nll, [1.0], bounds=[(0.05, 10.0)], method="L-BFGS-B")
        self.temperature = float(res.x[0])
        self._fitted = True
        return self

    def calibrate(self, probs_or_logits: np.ndarray) -> np.ndarray:
        if not self._fitted:
            raise RuntimeError("TemperatureScalingCalibrator has not been fitted.")

        probs = np.asarray(probs_or_logits, dtype=float)
        logits = self._probs_to_logits(probs)

        scaled = logits / self.temperature
        exp_scaled = np.exp(scaled - scaled.max(axis=1, keepdims=True))
        softmax_probs = exp_scaled / exp_scaled.sum(axis=1, keepdims=True)
        return softmax_probs

    def get_params(self) -> Dict[str, Any]:
        return {
            "method": CalibrationMethod.TEMPERATURE_SCALING.value,
            "fitted": self._fitted,
            "temperature": self.temperature,
        }


def get_calibrator(method: Union[CalibrationMethod, str] = CalibrationMethod.PLATT_SIGMOID) -> MulticlassCalibrator:
    m = CalibrationMethod(method)
    if m == CalibrationMethod.NONE:
        return UncalibratedWrapper()
    elif m == CalibrationMethod.PLATT_SIGMOID:
        return PlattSigmoidCalibrator()
    elif m == CalibrationMethod.TEMPERATURE_SCALING:
        return TemperatureScalingCalibrator()
    else:
        raise ValueError(f"Unknown calibration method: {method}")
