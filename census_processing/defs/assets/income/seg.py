import numpy as np
import pandas as pd
from scipy import integrate
from scipy.special import xlogy


def binary_entropy(p):
    """Compute binary entropy elementwise for probabilities in p.

    Entropy is defined as ``-p*log2(p) - (1-p)*log2(1-p)`` for values in the \
    open interval ``(0, 1)``. Values outside that interval return ``0``.

    Args:
        p: Scalar or array-like of probabilities.

    Returns:
        numpy.ndarray: Array of binary entropy values with the same shape as \
        ``p`` after conversion with ``numpy.asanyarray``.
    """
    p = np.asanyarray(p)
    e = np.zeros_like(p)
    mask = np.logical_and(p > 0, p < 1)
    pm = p[mask]
    e[mask] = -pm * np.log2(pm) - (1 - pm) * np.log2(1 - pm)
    return e


def local_binary_KL(p_local, p_global):
    """Compute local binary KL divergence against a global probability.

    The divergence is computed elementwise as:
    ``KL(p_local || p_global) = p_local*log2(p_local/p_global) + \
    (1-p_local)*log2((1-p_local)/(1-p_global))``.

    Args:
        p_local: Scalar or array-like of local probabilities.
        p_global: Scalar or array-like of reference global probabilities, \
            broadcastable to ``p_local``.

    Returns:
        numpy.ndarray: Elementwise KL divergence values in base-2 units.
    """
    p_local = np.asarray(p_local)
    p_global = np.asarray(p_global)

    KL = xlogy(p_local, p_local / p_global)
    # KL = p_local*np.log2(p_local/p_global)
    KL += xlogy(1 - p_local, (1 - p_local) / (1 - p_global))
    # KL += (1 - p_local)*np.log2((1 - p_local)/(1 - p_global))

    return KL / np.log(2)


def global_H_index(df_ind: pd.DataFrame, agebs: list[str]):
    """Compute the global segregation profile and global H index.

    The function builds the income distribution for each area (``p(y|n)``), \
    derives cumulative distributions ``F(y|n)``, computes local binary KL \
    divergences relative to the metropolitan distribution, and then aggregates \
    them using area population shares. The final global index ``H`` is the \
    numerical integral of the expected KL profile over the global income CDF.

    Args:
        df_ind: Individual-level weighted table containing at least \
            ``Ingreso_orig``, ``w_MZ``, and one column per area in ``agebs``. \
            Area columns are interpreted as weights by area.
        agebs: List of area identifiers (column names in ``df_ind``) to include \
            in the segregation computation.

    Returns:
        tuple: A 5-element tuple with:
            - float: Global segregation index ``H``.
            - pandas.DataFrame: ``df_cdf`` with cumulative income distributions \
                for each area and ``w_MZ``.
            - pandas.Series: ``norm_H_series``, expected KL normalized by binary \
                entropy of the global CDF (excluding the last point).
            - pandas.Series: ``mean_kl_series``, expected KL across areas for \
                each global percentile (excluding the last point).
            - pandas.DataFrame: ``local_kl`` by area, indexed by global CDF \
                values (excluding the final point where CDF equals 1).
    """
    # The probability distribution of income for each ageb, p(y|n)
    # with n indexing agebs
    df_prob = df_ind.reset_index(drop=True).groupby("Ingreso_orig").sum()
    df_prob = df_prob / df_prob.sum()

    # The cdf F(y|n) = \sum_{y'=0}^y p(y|n)
    df_cdf = df_prob.cumsum()

    # The local deviations for each ageb, for all percentiles of global
    # income distribution, (E(p) - E(p_n))/E(p)
    # local_deviations = df_cdf[agebs].apply(
    #     lambda x: local_bin_normalized_dev(x, df_cdf.w_MZ))
    local_kl = df_cdf[agebs].apply(lambda x: local_binary_KL(x, df_cdf.w_MZ))

    # Since the last row corresponds to F(y) = 1,
    # and reflects a single group, we drop it
    # local_deviations.drop(local_deviations.tail(1).index, inplace=True)
    local_kl = local_kl.drop(local_kl.tail(1).index)

    # The fraction population of each ageb are the
    # probabilities p(n)
    pn = df_ind[agebs].sum() / df_ind.w_MZ.sum()

    # The entropy indices for each percentile are a weighted mean
    # of local deviations
    # entropy_index_df = local_deviations.multiply(pn).sum(axis=1)
    mean_kl_series = local_kl.multiply(pn).sum(axis=1)
    norm_H_series = mean_kl_series / binary_entropy(df_cdf.w_MZ.to_numpy()[:-1])  # noqa: N806

    # But the above is not the function to integrate,
    # we must multiply by E(p) to recovr the
    # expected KL divergene.
    # Reme,ber remove last row
    # kl_df = entropy_index_df * binary_entropy(df_cdf.w_MZ.values[:-1])

    # Since the flutuations have been attenuated by the values of the
    # global entropy , it seems safe to integrate numerically the KL
    # function directly, despite the high level of noise at the tails
    # of H (see plots)
    H = integrate.simpson(y=mean_kl_series.values, x=df_cdf.w_MZ.to_numpy()[:-1])  # noqa: N806

    # Return the cdf, the local_h, the expected kl, and H
    return (
        H,
        df_cdf,
        norm_H_series,
        mean_kl_series,
        local_kl.set_index(df_cdf.w_MZ.iloc[:-1]),
    )
