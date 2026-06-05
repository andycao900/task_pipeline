defmodule TaskPipelineWeb.ErrorJSON do
  @moduledoc """
  This module is invoked by your endpoint in case of errors on JSON requests.

  See config/config.exs.
  """

  # If you want to customize a particular status code,
  # you may add your own clauses, such as:
  #
  # def render("500.json", _assigns) do
  #   %{errors: %{detail: "Internal Server Error"}}
  # end

  # By default, Phoenix returns the status message from
  # the template name. For example, "404.json" becomes
  # "Not Found".
  # Define an internal type contract matching the standard Phoenix JSON response envelope
  @type error_payload :: %{errors: %{detail: String.t()}}

  @spec render(String.t(), map()) :: error_payload()
  def render("404.json", _assigns) do
    %{errors: %{detail: "Not Found"}}
  end

  @spec render(String.t(), map()) :: error_payload()
  def render("500.json", _assigns) do
    %{errors: %{detail: "Internal Server Error"}}
  end

  @spec render(any(), any()) :: %{errors: %{detail: <<_::16, _::_*8>>}}
  def render(template, _assigns) do
    %{errors: %{detail: Phoenix.Controller.status_message_from_template(template)}}
  end
end
