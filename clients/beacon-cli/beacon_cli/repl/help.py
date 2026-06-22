"""Help text for the interactive shell's meta-commands."""

HELP_TEXT = """\
[bold]Meta-commands[/bold]
  \\dt                list tables
  \\dt+               list tables with kind / format / location
  \\d <table>         show a table's schema
  \\df                list scalar/aggregate functions
  \\dft               list table functions
  \\datasets (pat)    list datasets (optional glob)
  \\crawlers          list crawlers (SHOW CRAWLERS)
  \\run-crawler <n>   run a crawler once (admin)
  \\refresh <t>       refresh a table / materialized view (admin)
  \\info              server info
  \\format <fmt>      set export format (csv, parquet, ipc, netcdf, geoparquet, odv)
  \\export <file>     run the last statement and write it to <file>
  \\o <file>          alias for \\export
  \\limit <n>         max rows to render (-1 = all)
  \\x                 toggle expanded (field/value) display — good for wide tables
  \\timing            toggle timing display
  \\help, \\?          this help
  \\q, quit, exit     leave the shell

Anything else is sent as SQL; end a statement with ';' (or a blank line)."""
