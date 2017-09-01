use Error;

use std::ffi::CString;
use std::path::Path;

pub fn to_cpath(path: &Path) -> Result<CString, Error> {
    match CString::new(path.to_string_lossy().as_bytes()) {
        Ok(c) => Ok(c),
        Err(_) => {
            Err(Error::new(
                "Failed to convert path to CString when opening DB."
                    .to_owned(),
            ))
        }
    }
}
