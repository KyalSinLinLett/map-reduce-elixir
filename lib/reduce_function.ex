defmodule MapReduce.ReduceFunction do
  def reduce(key_value_pair_list, output_writer_pid) do
    send(output_writer_pid, {:reduce_key_put, self()})
    send_kv_pair_list(key_value_pair_list, output_writer_pid)
  end

  defp send_kv_pair_list([], _), do: IO.puts :stderr, "KV LIST EMPTY"

  defp send_kv_pair_list(kv_pair_list, output_writer_pid) do
    send(output_writer_pid, {:reduce_value_put, "#{elem(hd(kv_pair_list), 0)} #{Enum.reduce(kv_pair_list, 0, fn {_, value}, acc -> value + acc end)}"})
  end
end
