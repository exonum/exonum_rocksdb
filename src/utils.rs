// Copyright 2017 The Exonum Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use Error;
use Options;
use ffi::{rocksdb_list_column_families, rocksdb_list_column_families_destroy};

use libc::size_t;

use std::ffi::{CStr, CString};
use std::path::Path;
use std::slice;

pub fn to_cpath<P: AsRef<Path>>(path: P) -> Result<CString, Error> {
    match CString::new(path.as_ref().to_string_lossy().as_bytes()) {
        Ok(c) => Ok(c),
        Err(_) => {
            Err(Error::new(
                "Failed to convert path to CString when opening DB."
                    .to_owned()
            ))
        }
    }
}

pub fn get_cf_names<P: AsRef<Path>>(path: P) -> Result<Vec<String>, Error> {
    let opts = Options::default();
    let cpath = to_cpath(path)?;
    let result: Vec<String>;

    unsafe {
        let mut cflen: size_t = 0;
        let column_fams_raw = ffi_try!(rocksdb_list_column_families(
            opts.inner,
            cpath.as_ptr() as *const _,
            &mut cflen
        ));
        let column_fams = slice::from_raw_parts(column_fams_raw, cflen as usize);
        result = column_fams
            .iter()
            .map(|cf| CStr::from_ptr(*cf).to_string_lossy().into_owned())
            .collect();
        rocksdb_list_column_families_destroy(column_fams_raw, cflen);
    }

    Ok(result)
}
