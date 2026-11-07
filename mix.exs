defmodule BeamExport.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/HataluliRandima/beam_export"

  def project do
    [
      app: :beam_export,
      version: @version,
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      package: package(),
      description: description(),
      docs: docs(),
      deps: deps(),
      test_coverage: [
        # Test support code (repos, schemas, parsers) is not library code.
        ignore_modules: [~r/^BeamExport\.Test\./, BeamExport.DataCase],
        summary: [threshold: 90]
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      # :crypto generates unpredictable temporary file names.
      extra_applications: [:crypto]
    ]
  end

  defp deps do
    [
      {:telemetry, "~> 1.0"},
      # Decimal 3 is required by current Ecto releases; 2.x by older ones.
      {:decimal, "~> 2.0 or ~> 3.0", optional: true},
      {:ecto, "~> 3.10", optional: true},
      # Only referenced (behind a compile-time guard) to support the Ecto SQL
      # sandbox in users' test suites; declared so that it compiles first.
      {:ecto_sql, "~> 3.10", optional: true},
      {:plug, "~> 1.14", optional: true},

      # Test and Dev dependencies
      {:nimble_csv, "~> 1.2", only: :test},
      {:ecto_sqlite3, "~> 0.15", only: [:dev, :test]},
      {:benchee, "~> 1.0", only: [:dev, :test]},
      {:stream_data, "~> 1.0", only: [:dev, :test]},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
    ]
  end

  defp description do
    "Memory-bounded CSV exports from Ecto queries and enumerables to files, " <>
      "iodata and Plug/Phoenix downloads, without holding database connections " <>
      "for slow clients."
  end

  defp package do
    [
      maintainers: ["BeamExport Contributors"],
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/master/CHANGELOG.md"
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      source_url: @source_url,
      extras: ["README.md", "CHANGELOG.md", "ROADMAP.md", "LICENSE"],
      groups_for_modules: [
        Integrations: [BeamExport.Plug, BeamExport.Telemetry],
        Errors: [
          BeamExport.EncodeError,
          BeamExport.ExportError,
          BeamExport.MappingError,
          BeamExport.PreloadError,
          BeamExport.TimeoutError
        ]
      ]
    ]
  end
end
