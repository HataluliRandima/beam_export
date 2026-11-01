defmodule BeamExport.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/hata/beam_export"

  def project do
    [
      app: :beam_export,
      version: @version,
      elixir: "~> 1.14",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      package: package(),
      description: description(),
      docs: docs(),
      deps: deps()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:telemetry, "~> 1.0"},
      {:nimble_csv, "~> 1.2"},
      {:decimal, "~> 2.0", optional: true},
      {:ecto, "~> 3.10", optional: true},
      {:ecto_sql, "~> 3.10", optional: true},
      {:plug, "~> 1.14", optional: true},

      # Test and Dev dependencies
      {:ecto_sqlite3, "~> 0.15", only: [:dev, :test]},
      {:benchee, "~> 1.0", only: [:dev, :test]},
      {:stream_data, "~> 1.0", only: [:dev, :test]},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
    ]
  end

  defp description do
    "A production-grade, memory-bounded streaming data export engine for Elixir."
  end

  defp package do
    [
      maintainers: ["BeamExport Contributors"],
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      source_url: @source_url,
      extras: [
        "README.md",
        "ROADMAP.md",
        "CHANGELOG.md",
        "LICENSE",
        "docs/benchmark-audit.md": [title: "Benchmark Audit"],
        "docs/adversarial-review-final.md": [title: "Adversarial Review"]
      ]
    ]
  end
end
