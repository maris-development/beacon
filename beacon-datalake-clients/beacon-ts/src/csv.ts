/**
 * A small RFC 4180 CSV parser for the `query()` CSV fallback (used when
 * `apache-arrow` is not installed). Handles quoted fields, escaped quotes
 * (`""`), and commas/newlines inside quotes. All values are returned as
 * strings — the server's CSV output carries no type information.
 */

/** Parses CSV text into row objects keyed by the header row. */
export function parseCsv(text: string): Record<string, string>[] {
  const rows = parseCsvRows(text);
  const header = rows[0];
  if (!header) return [];
  return rows.slice(1).map((cells) => {
    const obj: Record<string, string> = {};
    header.forEach((name, i) => {
      obj[name] = cells[i] ?? "";
    });
    return obj;
  });
}

/** Parses CSV text into a 2D array of cell strings. */
export function parseCsvRows(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;
  let sawContent = false;

  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        field += c;
      }
      continue;
    }
    switch (c) {
      case '"':
        inQuotes = true;
        sawContent = true;
        break;
      case ",":
        row.push(field);
        field = "";
        sawContent = true;
        break;
      case "\r":
        break; // part of CRLF; the \n ends the row
      case "\n":
        row.push(field);
        rows.push(row);
        row = [];
        field = "";
        sawContent = false;
        break;
      default:
        field += c;
        sawContent = true;
    }
  }

  // Flush the trailing field/row unless the input ended exactly on a newline.
  if (sawContent || field !== "" || row.length > 0) {
    row.push(field);
    rows.push(row);
  }
  return rows;
}
