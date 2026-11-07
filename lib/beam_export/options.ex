defmodule BeamExport.Options do
  @moduledoc false
  # Validates the keyword options accepted by the public API.
  #
  # Unknown keys raise `ArgumentError` so that typos (for example
  # `sanitize_formula: true`) cannot silently disable a safety feature.
  # Validation happens before any side effect (telemetry, filesystem, database).

  @defaults [
    chunk_size: 500,
    separator: ",",
    line_terminator: "\r\n",
    sanitize_formulas: true,
    include_record_in_errors: false,
    max_rows: 500,
    timeout: :infinity,
    batch_timeout: 120_000
  ]

  @file_defaults [atomic: true, file_mode: 0o600]

  @common_keys [
    :headers,
    :mapper,
    :chunk_size,
    :separator,
    :line_terminator,
    :sanitize_formulas,
    :include_record_in_errors,
    :max_rows,
    :timeout,
    :consumer_timeout,
    :batch_timeout
  ]

  @file_keys [:atomic, :compress, :file_mode]
  @http_keys [:content_type, :charset]

  @keys %{
    to_stream: @common_keys,
    to_iodata: @common_keys,
    to_file: @common_keys ++ @file_keys,
    send_staged_export: @common_keys ++ @file_keys ++ @http_keys ++ [:tmp_dir],
    send_chunked_export: @common_keys ++ @http_keys
  }

  @doc false
  def http_keys(:send_staged_export), do: @http_keys ++ [:tmp_dir]
  def http_keys(:send_chunked_export), do: @http_keys

  @doc """
  Validates `opts` for the given API `kind` and returns them with defaults applied.
  Raises `ArgumentError` on unknown keys or invalid values.
  """
  @spec validate!(term(), atom()) :: keyword()
  def validate!(opts, kind) do
    unless is_list(opts) and Keyword.keyword?(opts) do
      raise ArgumentError, "BeamExport options must be a keyword list, got: #{inspect(opts)}"
    end

    allowed = Map.fetch!(@keys, kind)

    for {key, value} <- opts do
      unless key in allowed do
        raise ArgumentError, unknown_key_message(key, kind, allowed)
      end

      validate_value!(key, value)
    end

    defaults =
      if :atomic in allowed, do: @defaults ++ @file_defaults, else: @defaults

    Keyword.merge(defaults, opts)
  end

  defp unknown_key_message(key, kind, allowed) do
    suggestion =
      allowed
      |> Enum.map(&{&1, String.jaro_distance(to_string(key), to_string(&1))})
      |> Enum.max_by(&elem(&1, 1))
      |> case do
        {candidate, score} when score >= 0.8 -> " Did you mean #{inspect(candidate)}?"
        _ -> ""
      end

    "unknown option #{inspect(key)} for BeamExport #{kind}.#{suggestion} " <>
      "Valid options: #{Enum.map_join(allowed, ", ", &inspect/1)}"
  end

  defp validate_value!(:headers, nil), do: :ok

  defp validate_value!(:headers, value) when is_list(value) do
    if Enum.all?(value, &(is_binary(&1) or is_atom(&1))) do
      :ok
    else
      raise ArgumentError,
            "invalid :headers #{inspect(value)}: every header must be a string or an atom"
    end
  end

  defp validate_value!(:mapper, value) when is_nil(value) or is_function(value, 1), do: :ok

  defp validate_value!(key, value)
       when key in [:chunk_size, :max_rows] and is_integer(value) and value > 0,
       do: :ok

  defp validate_value!(:separator, value) when is_binary(value) and value != "" do
    if String.contains?(value, ["\"", "\r", "\n"]) do
      raise ArgumentError,
            "invalid :separator #{inspect(value)}: it must not contain a double quote, " <>
              "carriage return or line feed, as that would produce invalid CSV"
    end

    :ok
  end

  defp validate_value!(:line_terminator, value) when value in ["\r\n", "\n"], do: :ok

  defp validate_value!(key, value)
       when key in [:sanitize_formulas, :include_record_in_errors, :atomic] and
              is_boolean(value),
       do: :ok

  defp validate_value!(:timeout, value)
       when value == :infinity or (is_integer(value) and value > 0),
       do: :ok

  defp validate_value!(key, value)
       when key in [:consumer_timeout, :batch_timeout] and
              (value == :infinity or (is_integer(value) and value > 0)),
       do: :ok

  defp validate_value!(:compress, value) when value in [nil, true, false, :gzip], do: :ok

  defp validate_value!(:file_mode, value)
       when is_integer(value) and value >= 0 and value <= 0o777,
       do: :ok

  defp validate_value!(key, value)
       when key in [:tmp_dir, :content_type] and is_binary(value) and value != "",
       do: :ok

  defp validate_value!(:charset, value) when is_nil(value) or is_binary(value), do: :ok

  defp validate_value!(key, value) do
    raise ArgumentError,
          "invalid value for BeamExport option #{inspect(key)}: #{inspect(value)}. " <>
            expected(key)
  end

  defp expected(:headers), do: "Expected a list or nil"
  defp expected(:mapper), do: "Expected a function of arity 1 or nil"
  defp expected(:separator), do: "Expected a non-empty string"
  defp expected(:line_terminator), do: ~s(Expected "\\r\\n" or "\\n")
  defp expected(:timeout), do: "Expected a positive integer (milliseconds) or :infinity"
  defp expected(:compress), do: "Expected true, false, :gzip or nil"
  defp expected(:file_mode), do: "Expected an integer between 0 and 0o777"
  defp expected(:charset), do: "Expected a string or nil"

  defp expected(key) when key in [:consumer_timeout, :batch_timeout],
    do: "Expected a positive integer (milliseconds) or :infinity"

  defp expected(key) when key in [:chunk_size, :max_rows], do: "Expected a positive integer"

  defp expected(key) when key in [:sanitize_formulas, :include_record_in_errors, :atomic],
    do: "Expected a boolean"

  defp expected(_key), do: "Expected a non-empty string"
end
