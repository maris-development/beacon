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
  \\i <file.sql>      run a SQL script (multiple ;-separated statements)
  \\limit <n>         max rows to render (-1 = all)
  \\x                 toggle expanded (field/value) display — good for wide tables
  \\timing            toggle timing display
  \\vi, \\emacs        switch key bindings (vi / emacs)
  \\help, \\?          this help
  \\q, quit, exit     leave the shell

Statements are multi-line: Enter adds a line, cursor keys move between lines, and
';' (or a blank line, or Alt+Enter) runs it. [bold]Ctrl+F[/bold] (or F2)
pretty-prints the SQL across lines; [bold]Ctrl+C[/bold] cancels a running query.
Completions show as you type; press [bold]Tab[/bold] to open/insert (Shift+Tab to
cycle back). Ctrl+Space also opens the menu where the terminal supports it. The
status bar at the bottom shows the connection, your identity, and the current
row-limit / export format / timing / editing mode."""
