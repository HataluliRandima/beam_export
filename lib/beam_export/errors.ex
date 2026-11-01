defmodule BeamExport.MappingError do
  defexception [:message, :record, :original_error]

  @impl true
  def exception(opts) do
    record = Keyword.get(opts, :record)
    orig = Keyword.get(opts, :original_error)
    msg = "Failed to map record for export: #{inspect(orig)}\nRecord: #{inspect(record)}"
    %__MODULE__{message: msg, record: record, original_error: orig}
  end
end

defmodule BeamExport.PreloadError do
  defexception [:message]

  @impl true
  def exception(opts) do
    msg =
      Keyword.get(
        opts,
        :message,
        "Preloaded associations are not supported in Ecto streaming. " <>
          "Please load associations using database joins or select fields directly."
      )

    %__MODULE__{message: msg}
  end
end

