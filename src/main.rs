use calamine::{open_workbook_auto,  Reader};
use polars::{
    prelude::{DataFrame, IntoColumn, NamedFrom}, series::Series
};
use rayon::prelude::*;
use std::collections::HashMap;

#[derive(Debug)]
pub enum SheetName<'a>{
    Name(&'a str),
    Names(Vec<&'a str>),
}

#[derive(Debug)]
pub enum SheetIndex {
    Index(usize),
    Indices(Vec<usize>),
}
#[derive(Debug)]
pub enum SheetData {
    SingleSheet(calamine::Range<calamine::Data>),
    MultiSheet(HashMap<String,calamine::Range<calamine::Data>>),
}

#[derive(Debug)]
pub enum ExcelDataFrame{
    SingleSheet(DataFrame),
    MultiSheet(HashMap<String,DataFrame>),
}

fn main() {
    // println!("{:#?}",read_excel("Book1.xlsx", Some(SheetName::Names(vec!["Sheet1","Sheet2"])), None));
    println!("{:#?}",read_excel("Book1.xlsx", None, Some(SheetIndex::Index(1))));
}

pub fn read_excel(path: &str, sheet_name: Option<SheetName>, sheet_index: Option<SheetIndex>)  -> ExcelDataFrame{
    if sheet_name.is_some() && sheet_index.is_some() {
        panic!("Cannot specify both sheet_name and sheet_index. Please choose one.");
    }
    let mut wb  = open_workbook_auto(path).unwrap();
    let names = wb.sheet_names();
    let sheet:SheetData = match sheet_index {
        Some(SheetIndex::Index(sheet_index)) => {
            let name  = names.get(sheet_index).unwrap();
            SheetData::SingleSheet(wb.worksheet_range(name).unwrap())
        },
        Some(SheetIndex::Indices(sheet_index))=>{

        let sheet_name_vector:Vec<String>=sheet_index.par_iter().map(|x|{
                names.get(*x).unwrap().clone()

            }).collect();  
        iter_sheet_names(sheet_name_vector, wb)
        }
        None => match sheet_name {
            Some(SheetName::Name(sheet_name)) => SheetData::SingleSheet(wb.worksheet_range(sheet_name).unwrap()),
            Some(SheetName::Names(sheet_name)) =>
                iter_sheet_names
                (sheet_name.iter().map(|x|{x.to_string()}).collect(), wb),
            None => {
                let name = names.get(0).unwrap();
                SheetData::SingleSheet(wb.worksheet_range(name).unwrap())
            }
        },
    };
    sheet_data_to_dataframe(sheet)
}

fn iter_sheet_names(names:Vec<String>,mut workbook:calamine::Sheets<std::io::BufReader<std::fs::File>>)->SheetData{
    let sheets = HashMap::from_iter(names.iter().map(|name|{
            (name.to_owned(), workbook.worksheet_range(name).unwrap())
            }));
    SheetData::MultiSheet(sheets)
}
fn filter_data(
    data: Vec<(usize, usize, calamine::Data)>,
    columns_count: &usize,
) -> polars::prelude::DataFrame {
    let column_indices: Vec<u8> = (0..*columns_count as u8).collect();
    let x: Vec<polars::prelude::Column> = column_indices
        .par_iter()
        .map(|x| excel_column_to_pl_series(&data, *x as usize))
        .collect();

    DataFrame::new(x).unwrap()
}

fn excel_column_to_pl_series(
    data: &Vec<(usize, usize, calamine::Data)>,
    column: usize,
) -> polars::prelude::Column {
    let values: Vec<Option<String>> = data
        .into_par_iter()
        .filter(|data| {
            let (_row, col, _cell_value) = data;
            *col == column
        })
        .map(|(_row, _column, cell_value)| {
            let val_str = match cell_value {
                calamine::Data::Empty => None,
                calamine::Data::String(s) => Some(s.clone()), // Clone the string
                calamine::Data::Float(f) => Some(f.to_string()),
                calamine::Data::Int(i) => Some(i.to_string()),
                calamine::Data::Bool(b) => Some(b.to_string()),
                calamine::Data::DateTime(d) => Some(d.to_string()),
                _ => Some(format!("{:?}", cell_value)), // Fallback for other types
            };
            val_str
        })
        .collect();
    let name = values.first().unwrap().as_ref().unwrap();
    let values = values.get(1..).unwrap().to_vec();
    Series::new(name.into(), values).into_column()
}


fn sheet_data_to_dataframe(sheetdata:SheetData)->ExcelDataFrame{
    match  sheetdata{
        SheetData::SingleSheet(sheet)=>ExcelDataFrame::SingleSheet(process_single_sheet(sheet)),
        SheetData::MultiSheet(sheets)=>ExcelDataFrame::MultiSheet(process_multi_sheet(sheets))
    }
    
}

fn process_single_sheet(data:calamine::Range<calamine::Data>) -> polars::prelude::DataFrame{
    let columns = &data.headers().unwrap();
    let data: Vec<(usize, usize, calamine::Data)> = data
    .cells()
    .map(|(row, col, cell_data_ref)| (row, col, cell_data_ref.clone()))
    .collect();
    filter_data(data, &columns.len())
}

fn process_multi_sheet(data:HashMap<String,calamine::Range<calamine::Data>>)->HashMap<String,DataFrame>{
    
    HashMap::from_par_iter(data.par_iter().
    map(|(key,value)|{
        (key.clone(),process_single_sheet(value.clone()))
    }))
    
}