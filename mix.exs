defmodule RedixStream.Mixfile do
  use Mix.Project

  def project do
    [
      app: :redix_stream,
      version: "0.1.6",
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
      {:redix, "~> 0.8.2"},
      {:ex_doc, "~> 0.19", only: :dev, runtime: false},
      {:dialyxir, "~> 1.0.0-rc.3", only: [:dev], runtime: false}
    ]
  end
end
