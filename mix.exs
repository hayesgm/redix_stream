defmodule RedixStream.Mixfile do
  use Mix.Project

  def project do
    [
      app: :redix_stream,
      version: "0.2.3",
      description: "Redis Stream Processor in Elxir, built on redix",
      package: [
        maintainers: ["Geoffrey Hayes"],
        licenses: ["MIT"],
        links: %{"GitHub" => "https://github.com/hayesgm/redix_stream"}
      ],
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:redix, "~> 1.1"},
      {:ex_doc, "~> 0.28", only: :dev, runtime: false},
      {:dialyxir, "~> 1.1", only: [:dev], runtime: false},
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false}
    ]
  end
end
