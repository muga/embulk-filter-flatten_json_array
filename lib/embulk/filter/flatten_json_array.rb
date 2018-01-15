Embulk::JavaPlugin.register_filter(
  "json_array_flatten", "org.embulk.filter.flatten_json_array.FlattenJsonArrayFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
