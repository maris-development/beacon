use std::{collections::HashMap, error::Error};

use lazy_static::lazy_static;

const EDMO_MAPPINGS_CSV: &[u8] = include_bytes!("edmo.csv");

lazy_static! {
    static ref EDMO_MAP: HashMap<(String, String), String> =
        read_from_to_mappings_from_reader(EDMO_MAPPINGS_CSV.as_ref()).unwrap();
}

pub fn read_from_to_mappings_from_reader<R: std::io::Read>(
    reader: R,
) -> Result<HashMap<(String, String), String>, Box<dyn Error + Send + Sync>> {
    let mut rdr = csv::Reader::from_reader(reader);

    let mut map: HashMap<(String, String), String> = HashMap::new();

    for record in rdr.records().flatten() {
        let key_country = record.get(9).unwrap_or("").trim();
        let key_name = record.get(1).unwrap_or("").trim();
        let value = record.get(0).unwrap_or("").trim();

        if !value.is_empty() {
            map.insert(
                (
                    key_country.to_lowercase().to_string(),
                    key_name.to_lowercase().to_string(),
                ),
                value.to_string(),
            );
        }
    }

    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name() {
        println!("EDMO Mappings: {:?}", *EDMO_MAP);
    }
}
