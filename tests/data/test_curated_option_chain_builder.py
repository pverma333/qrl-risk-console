import pytest
import numpy as np
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch
from src.data.curated_option_chain_builder import CuratedOptionChainBuilder

@pytest.fixture
def mock_config(tmp_path):
    config = MagicMock()
    config.curated_dir = tmp_path / "curated"
    config.logs_dir    = tmp_path / "logs"
    config.duckdb_path = tmp_path / "test.db"
    config.logs_dir.mkdir(parents=True)
    config.curated_dir.mkdir(parents=True)
    return config

@pytest.fixture
def builder(mock_config):
    with patch("src.data.curated_option_chain_builder.DuckDBConnection"), \
         patch("src.data.curated_option_chain_builder.ProcessedRegistry"):
        b = CuratedOptionChainBuilder(mock_config)
        b.con = MagicMock()
    return b

@pytest.fixture
def sample_df():
    """Realistic option chain slice — ATM NIFTY call."""
    return pd.DataFrame([
        {
            "trade_date":   pd.Timestamp("2024-10-15"),
            "symbol":       "NIFTY",
            "expiry_date":  pd.Timestamp("2024-10-31"),
            "strike":       22000.0,
            "option_type":  "CE",
            "open":         310.0,
            "high":         320.0,
            "low":          295.0,
            "close":        305.0,
            "settle":       300.0,
            "contracts":    1500,
            "open_interest": 50000,
            "chg_in_oi":    200,
            "dte":          16,
            "spot":         22100.0,
            "div_yield":    1.23,
            "rate_3m":      6.44,
            "rate_6m":      6.56,
            "rate_1y":      6.566,
        }
    ])


@pytest.fixture
def multi_row_df():
    """Multi-row DataFrame with valid and invalid rows."""
    rows = []
    strikes = [20000, 21000, 22000, 23000, 24000]
    for K in strikes:
        rows.append({
            "trade_date":    pd.Timestamp("2024-10-15"),
            "symbol":        "NIFTY",
            "expiry_date":   pd.Timestamp("2024-10-31"),
            "strike":        float(K),
            "option_type":   "CE",
            "open":          300.0,
            "high":          320.0,
            "low":           280.0,
            "close":         300.0,
            "settle":        max(22000.0 - K + 300.0, 1.0),
            "contracts":     1000,
            "open_interest": 40000,
            "chg_in_oi":     100,
            "dte":           16,
            "spot":          22000.0,
            "div_yield":     1.23,
            "rate_3m":       6.44,
            "rate_6m":       6.56,
            "rate_1y":       6.566,
        })
    # add one invalid row — zero settle
    rows.append({
        "trade_date":    pd.Timestamp("2024-10-15"),
        "symbol":        "NIFTY",
        "expiry_date":   pd.Timestamp("2024-10-31"),
        "strike":        26000.0,
        "option_type":   "CE",
        "open":          0.0, "high": 0.0, "low": 0.0, "close": 0.0,
        "settle":        0.0,
        "contracts":     0, "open_interest": 0, "chg_in_oi": 0,
        "dte":           16,
        "spot":          22000.0,
        "div_yield":     1.23,
        "rate_3m":       6.44, "rate_6m": 6.56, "rate_1y": 6.566,
    })
    return pd.DataFrame(rows)

# Rate Interpolation

class TestInterpolateRates:

    def test_rate_column_added(self, builder, sample_df):
        result = builder._interpolate_rates(sample_df)
        assert "rate" in result.columns

    def test_rate_is_decimal(self, builder, sample_df):
        result = builder._interpolate_rates(sample_df)
        assert result["rate"].iloc[0] < 1.0

    def test_rate_below_3m_flat(self, builder, sample_df):
        # dte=16 < 91 → should use 3M rate
        df = sample_df.copy()
        df["dte"] = 16
        result = builder._interpolate_rates(df)
        expected = 6.44 / 100
        assert result["rate"].iloc[0] == pytest.approx(expected, abs=1e-6)

    def test_rate_between_3m_6m(self, builder, sample_df):
        df = sample_df.copy()
        df["dte"] = 120
        result = builder._interpolate_rates(df)
        assert 6.44/100 < result["rate"].iloc[0] < 6.56/100

    def test_rate_between_6m_1y(self, builder, sample_df):
        df = sample_df.copy()
        df["dte"] = 270
        result = builder._interpolate_rates(df)
        assert 6.56/100 < result["rate"].iloc[0] < 6.566/100

    def test_rate_above_1y_flat(self, builder, sample_df):
        df = sample_df.copy()
        df["dte"] = 400
        result = builder._interpolate_rates(df)
        expected = 6.566 / 100
        assert result["rate"].iloc[0] == pytest.approx(expected, abs=1e-6)

    def test_original_df_not_mutated(self, builder, sample_df):
        original_cols = set(sample_df.columns)
        _ = builder._interpolate_rates(sample_df)
        assert set(sample_df.columns) == original_cols

    def test_batch_interpolation(self, builder, multi_row_df):
        result = builder._interpolate_rates(multi_row_df)
        assert len(result) == len(multi_row_df)
        assert result["rate"].notna().all()


# Quant Computation

class TestComputeQuant:

    def _with_rate(self, builder, df):
        df = builder._interpolate_rates(df)
        return builder._compute_quant(df)

    def test_quant_columns_added(self, builder, sample_df):
        result = self._with_rate(builder, sample_df)
        for col in ["iv", "delta", "gamma", "vega", "theta", "rho"]:
            assert col in result.columns

    def test_valid_row_iv_not_nan(self, builder, sample_df):
        result = self._with_rate(builder, sample_df)
        assert not np.isnan(result["iv"].iloc[0])

    def test_valid_row_all_greeks_not_nan(self, builder, sample_df):
        result = self._with_rate(builder, sample_df)
        for col in ["delta", "gamma", "vega", "theta", "rho"]:
            assert not np.isnan(result[col].iloc[0])

    def test_zero_settle_row_iv_nan(self, builder, multi_row_df):
        result = self._with_rate(builder, multi_row_df)
        zero_settle_mask = multi_row_df["settle"] == 0.0
        assert result.loc[zero_settle_mask.values, "iv"].isna().all()

    def test_call_delta_positive(self, builder, sample_df):
        result = self._with_rate(builder, sample_df)
        assert result["delta"].iloc[0] > 0

    def test_put_delta_negative(self, builder, sample_df):
        df = sample_df.copy()
        df["option_type"] = "PE"
        df["settle"] = 280.0
        result = self._with_rate(builder, df)
        assert result["delta"].iloc[0] < 0

    def test_div_yield_converted_correctly(self, builder, sample_df):
        # div_yield=1.23 in df should be treated as 0.0123 in BS
        # verify IV is reasonable (not distorted by 120% dividend yield)
        result = self._with_rate(builder, sample_df)
        if not np.isnan(result["iv"].iloc[0]):
            assert result["iv"].iloc[0] < 2.0  # IV below 200%

    def test_original_df_not_mutated(self, builder, sample_df):
        df = builder._interpolate_rates(sample_df)
        original_cols = set(df.columns)
        _ = builder._compute_quant(df)
        assert set(df.columns) == original_cols


# Drop Rate Tenor Columns

class TestDropRateTenorCols:

    def test_tenor_cols_removed(self, builder, sample_df):
        df = builder._interpolate_rates(sample_df)
        result = builder._drop_rate_tenor_cols(df)
        for col in ["rate_3m", "rate_6m", "rate_1y"]:
            assert col not in result.columns

    def test_rate_col_retained(self, builder, sample_df):
        df = builder._interpolate_rates(sample_df)
        result = builder._drop_rate_tenor_cols(df)
        assert "rate" in result.columns

    def test_other_cols_retained(self, builder, sample_df):
        df = builder._interpolate_rates(sample_df)
        result = builder._drop_rate_tenor_cols(df)
        for col in ["trade_date", "symbol", "strike", "settle"]:
            assert col in result.columns


# Deduplication

class TestDeduplicate:

    def test_no_duplicates_unchanged(self, builder, sample_df):
        result = builder._deduplicate(sample_df)
        assert len(result) == len(sample_df)

    def test_duplicates_removed(self, builder, sample_df):
        df = pd.concat([sample_df, sample_df], ignore_index=True)
        result = builder._deduplicate(df)
        assert len(result) == 1

    def test_different_strikes_not_deduped(self, builder, sample_df):
        df2 = sample_df.copy()
        df2["strike"] = 23000.0
        combined = pd.concat([sample_df, df2], ignore_index=True)
        result = builder._deduplicate(combined)
        assert len(result) == 2

    def test_different_option_types_not_deduped(self, builder, sample_df):
        df2 = sample_df.copy()
        df2["option_type"] = "PE"
        combined = pd.concat([sample_df, df2], ignore_index=True)
        result = builder._deduplicate(combined)
        assert len(result) == 2


# Schema Validation

class TestValidateSchema:

    def _full_df(self, builder, sample_df):
        df = builder._interpolate_rates(sample_df)
        df = builder._compute_quant(df)
        df = builder._drop_rate_tenor_cols(df)
        return df

    def test_valid_schema_passes(self, builder, sample_df):
        df = self._full_df(builder, sample_df)
        builder._validate_schema(df)  # should not raise

    def test_missing_column_raises(self, builder, sample_df):
        df = self._full_df(builder, sample_df)
        df = df.drop(columns=["iv"])
        with pytest.raises(ValueError, match="Missing columns"):
            builder._validate_schema(df)

    def test_null_trade_date_raises(self, builder, sample_df):
        df = self._full_df(builder, sample_df)
        df["trade_date"] = None
        with pytest.raises(ValueError, match="trade_date"):
            builder._validate_schema(df)

    def test_null_symbol_raises(self, builder, sample_df):
        df = self._full_df(builder, sample_df)
        df["symbol"] = None
        with pytest.raises(ValueError, match="symbol"):
            builder._validate_schema(df)

    def test_null_iv_does_not_raise(self, builder, sample_df):
        df = self._full_df(builder, sample_df)
        df["iv"] = np.nan
        builder._validate_schema(df)  # iv nulls are allowed


# Write Partitioned

class TestWritePartitioned:

    def _full_df(self, builder, sample_df):
        df = builder._interpolate_rates(sample_df)
        df = builder._compute_quant(df)
        df = builder._drop_rate_tenor_cols(df)
        return df

    def test_full_mode_writes_parquet(self, builder, sample_df):
        df = self._full_df(builder, sample_df)
        builder._write_partitioned(df, 2024, "full")
        out_path = builder.output_root / "2024" / "curated_options_2024.parquet"
        assert out_path.exists()

    def test_full_mode_correct_row_count(self, builder, sample_df):
        df = self._full_df(builder, sample_df)
        builder._write_partitioned(df, 2024, "full")
        out_path = builder.output_root / "2024" / "curated_options_2024.parquet"
        result = pd.read_parquet(out_path)
        assert len(result) == 1

    def test_incremental_mode_appends(self, builder, sample_df):
        df = self._full_df(builder, sample_df)
        builder._write_partitioned(df, 2024, "full")

        df2 = sample_df.copy()
        df2["trade_date"] = pd.Timestamp("2024-10-16")
        df2["strike"] = 22500.0
        df2 = self._full_df(builder, df2)
        builder._write_partitioned(df2, 2024, "incremental")

        out_path = builder.output_root / "2024" / "curated_options_2024.parquet"
        result = pd.read_parquet(out_path)
        assert len(result) == 2

    def test_incremental_mode_deduplicates(self, builder, sample_df):
        df = self._full_df(builder, sample_df)
        builder._write_partitioned(df, 2024, "full")
        builder._write_partitioned(df, 2024, "incremental")

        out_path = builder.output_root / "2024" / "curated_options_2024.parquet"
        result = pd.read_parquet(out_path)
        assert len(result) == 1

    def test_output_sorted_by_trade_date(self, builder, sample_df):
        df = self._full_df(builder, sample_df)
        builder._write_partitioned(df, 2024, "full")
        out_path = builder.output_root / "2024" / "curated_options_2024.parquet"
        result = pd.read_parquet(out_path)
        dates = pd.to_datetime(result["trade_date"])
        assert dates.is_monotonic_increasing

    def test_no_mixed_type_error_on_incremental(self, builder, sample_df):
        df = self._full_df(builder, sample_df)
        builder._write_partitioned(df, 2024, "full")

        df2 = sample_df.copy()
        df2["trade_date"] = pd.Timestamp("2024-10-16")
        df2 = self._full_df(builder, df2)
        # should not raise TypeError from mixed Timestamp/date types
        builder._write_partitioned(df2, 2024, "incremental")
