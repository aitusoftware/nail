use chrono::prelude::*;
use std::fs::*;
use std::io::Read;
use std::io::Write;
use std::ops::Rem;
use std::time::*;

const DIGITS_IN_EPOCH_SECOND_TIMESTAMP: usize = 10;
const DIGITS_IN_EPOCH_MILLISECOND_TIMESTAMP: usize = 13;
const NANOS_PER_MILLISECOND: i64 = 1_000_000;
const BUFFER_SIZE: usize = 1024;
const ASCII_ZERO: u8 = 48;
const ASCII_LOWERCASE_A: u8 = 97;
const ASCII_UPPERCASE_A: u8 = 65;
const ASCII_LOWERCASE_F: u8 = 102;
const ASCII_UPPERCASE_F: u8 = 70;
const ASCII_LOWERCASE_X: u8 = 120;


pub fn enhex(tokens: &[String]) {
    for token in tokens {
        let mut tmp = Vec::new();
        tmp.extend_from_slice(token.as_bytes());
        // TODO validate that all chars are ascii_digits
        println!("{}", to_hex_chars(ascii_to_integer(&mut tmp)))
    }
}

pub fn dehex(tokens: &[String]) {
    for token in tokens {
        // TODO validate that all chars are hex_ascii_digits
        println!("{}", to_decimal_chars(token))
    }
}

fn to_hex_chars(input: u64) -> String {
    format!("0x{:x}", input)
}

fn to_decimal_chars(input: &String) -> String {
    let mut value: u64 = 0;
    let chars = input.as_bytes();
    let start = if has_hex_indicator(chars) { 2 } else { 0 };
    for i in start..chars.len() {
        value *= 16;
        let index = i;
        let hex_char = chars[index];
        if hex_char >= ASCII_UPPERCASE_A && hex_char <= ASCII_UPPERCASE_F {
            let v = 10 + (hex_char - ASCII_UPPERCASE_A) as u64;
            value += v
        } else if hex_char >= ASCII_LOWERCASE_A && hex_char <= ASCII_LOWERCASE_F {
            let v = 10 + (hex_char - ASCII_LOWERCASE_A) as u64;
            value += v
        } else {
            let v = (hex_char - ASCII_ZERO) as u64;
            value += v
        }
    }
    return format!("{}", value)
}

fn has_hex_indicator(chars: &[u8]) -> bool {
    chars.len() > 1 && chars[0] == ASCII_ZERO && chars[1] == ASCII_LOWERCASE_X
}

fn hex_ascii_to_integer(integer_accumulator: &mut Vec<u8>) -> u64 {
    let mut value: u64 = 0;
    integer_accumulator.reverse();
    loop {
        if let Some(next) = integer_accumulator.pop() {
            value *= 10;
            value += (next - ASCII_ZERO) as u64
        } else {
            break;
        }
    }
    value
}

fn ascii_to_integer(integer_accumulator: &mut Vec<u8>) -> u64 {
    let mut value: u64 = 0;
    integer_accumulator.reverse();
    loop {
        if let Some(next) = integer_accumulator.pop() {
            value *= 10;
            value += (next - ASCII_ZERO) as u64
        } else {
            break;
        }
    }
    value
}


pub fn process_files(files: &[String]) {
    let mut options = OpenOptions::new();
    options.read(true);
    let mut read_buffer = [0; BUFFER_SIZE];
    for file_name in files {
        let target_file_name = file_name.to_string() + &".depoch".to_string();
        let mut file = options.open(file_name).unwrap();
        let mut target_file = options
            .create(true)
            .write(true)
            .open(target_file_name)
            .unwrap();
        process_input(&mut file, &mut target_file, &mut read_buffer)
    }
}

pub fn process_stdin() {
    let stdin = std::io::stdin();
    let mut stdin_lock = stdin.lock();
    let stdout = std::io::stdout();
    let mut stdout_lock = stdout.lock();
    let mut read_buffer = [0; BUFFER_SIZE];

    process_input(&mut stdin_lock, &mut stdout_lock, &mut read_buffer)
}

fn process_input(input: &mut Read, output: &mut Write, read_buffer: &mut [u8]) {
    let mut data_buffer = Vec::new();
    let mut tmp_buffer = Vec::new();
    loop {
        let read_length = input
            .read(read_buffer)
            .expect("Error reading from input file");
        if read_length != 0 {
            let initial_length = data_buffer.len();
            data_buffer.extend_from_slice(&read_buffer);
            let replacement = replace_epoch_timestamps_in_buffer(
                &data_buffer,
                read_length + initial_length,
                read_length < read_buffer.len(),
            );
            let slice = replacement.data.as_slice();
            output.write_all(slice).expect("Failed to write");

            if replacement.left_over_data != 0 {
                for _ in 0..replacement.left_over_data {
                    tmp_buffer.push(data_buffer.pop().expect("Invalid state"))
                }
                tmp_buffer.reverse();
                data_buffer.clear();
                data_buffer.extend(&tmp_buffer);
            } else {
                data_buffer.clear();
            }
        } else {
            break;
        }
    }
    output.flush().expect("Error flushing output")
}

pub struct ReplacementResult {
    pub data: Vec<u8>,
    pub left_over_data: u64,
}

fn replace_epoch_timestamps(input: &Vec<u8>, end_of_input: bool) -> ReplacementResult {
    replace_epoch_timestamps_in_buffer(input, input.len(), end_of_input)
}

fn replace_epoch_timestamps_in_buffer(
    input: &Vec<u8>,
    input_length: usize,
    end_of_input: bool,
) -> ReplacementResult {
    let mut replaced: Vec<u8> = Vec::new();
    let mut integer_accumulator = Vec::new();
    for index in 0..input_length {
        if input[index].is_ascii_digit() {
            integer_accumulator.push(input[index]);
        } else {
            process_possible_timestamp(&mut integer_accumulator, &mut replaced);
            replaced.push(input[index]);
        }
    }
    if end_of_input {
        process_possible_timestamp(&mut integer_accumulator, &mut replaced);
    }

    if replaced.len() != 0 {
        ReplacementResult {
            data: replaced,
            left_over_data: integer_accumulator.len() as u64,
        }
    } else {
        ReplacementResult {
            data: input.to_vec(),
            left_over_data: integer_accumulator.len() as u64,
        }
    }
}

fn process_possible_timestamp(integer_accumulator: &mut Vec<u8>, replaced: &mut Vec<u8>) {
    if is_epoch_millisecond_timestamp(&integer_accumulator)
        || is_epoch_second_timestamp(&integer_accumulator)
    {
        append_epoch_timestamp(integer_accumulator, replaced)
    }
}

fn append_epoch_timestamp(integer_accumulator: &mut Vec<u8>, append_buffer: &mut Vec<u8>) {
    let digit_count = integer_accumulator.len();
    let timestamp: i64 = ascii_to_integer(integer_accumulator) as i64;

    let nanos: u32 = match digit_count {
        DIGITS_IN_EPOCH_MILLISECOND_TIMESTAMP => {
            (timestamp.rem(1000) as i64 * NANOS_PER_MILLISECOND) as u32
        }
        DIGITS_IN_EPOCH_SECOND_TIMESTAMP => 0 as u32,
        _ => panic!("Cannot handle {} digits", digit_count),
    };
    let seconds: i64 = match digit_count {
        DIGITS_IN_EPOCH_MILLISECOND_TIMESTAMP => timestamp / 1000,
        DIGITS_IN_EPOCH_SECOND_TIMESTAMP => timestamp,
        _ => panic!("Cannot handle {} digits", digit_count),
    };

    let date_time = Utc.timestamp(seconds, nanos);
    let timestamp_str = format!("[{}]", date_time);
    append_buffer.extend_from_slice(timestamp_str.as_bytes());
    integer_accumulator.clear()
}

fn is_epoch_millisecond_timestamp(input: &Vec<u8>) -> bool {
    return input.len() == DIGITS_IN_EPOCH_MILLISECOND_TIMESTAMP;
}

fn is_epoch_second_timestamp(input: &Vec<u8>) -> bool {
    return input.len() == DIGITS_IN_EPOCH_SECOND_TIMESTAMP;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convert_to_hex() {
        assert_eq!("0x9fbf1", to_hex_chars(654321))
    }

    #[test]
    fn convert_from_hex() {
        assert_eq!("654321", to_decimal_chars(&"9fbf1".to_string()))
    }

    #[test]
    fn convert_from_hex_with_leading_zero() {
        assert_eq!("654321", to_decimal_chars(&"09fbf1".to_string()))
    }

    #[test]
    fn convert_from_hex_with_leading_zero_and_hex_indicator() {
        assert_eq!("654321", to_decimal_chars(&"0x09fbf1".to_string()))
    }


    #[test]
    fn replace_valid_timestamp_with_millisecond_precision() {
        let input = "1530216070317a";
        let expected = "[2018-06-28 20:01:10.317 UTC]a";
        let mut input_buffer = Vec::new();
        input_buffer.extend_from_slice(&input.as_bytes());
        let response = replace_epoch_timestamps(&input_buffer, false);

        assert_eq!(0, response.left_over_data);
        compare_bytes(expected.as_bytes(), &response.data);
    }

    #[test]
    fn replace_valid_timestamp_with_second_precision() {
        let input = "1530216070a";
        let expected = "[2018-06-28 20:01:10 UTC]a";
        let mut input_buffer = Vec::new();
        input_buffer.extend_from_slice(&input.as_bytes());
        let response = replace_epoch_timestamps(&input_buffer, false);

        assert_eq!(0, response.left_over_data);
        compare_bytes(expected.as_bytes(), &response.data);
    }

    #[test]
    fn do_not_replace_millisecond_timestamp_at_end_of_input() {
        let input = "1530216070317";
        let expected = "1530216070317";
        let mut input_buffer = Vec::new();
        input_buffer.extend_from_slice(&input.as_bytes());
        let response = replace_epoch_timestamps(&input_buffer, false);

        assert_eq!(13, response.left_over_data);
        compare_bytes(expected.as_bytes(), &response.data);
    }

    #[test]
    fn do_not_replace_second_timestamp_at_end_of_input() {
        let input = "1530216070";
        let expected = "1530216070";
        let mut input_buffer = Vec::new();
        input_buffer.extend_from_slice(&input.as_bytes());
        let response = replace_epoch_timestamps(&input_buffer, false);

        assert_eq!(10, response.left_over_data);
        compare_bytes(expected.as_bytes(), &response.data);
    }

    #[test]
    fn replace_millisecond_timestamp_at_end_of_input() {
        let input = "1530216070317";
        let expected = "[2018-06-28 20:01:10.317 UTC]";
        let mut input_buffer = Vec::new();
        input_buffer.extend_from_slice(&input.as_bytes());
        let response = replace_epoch_timestamps(&input_buffer, true);

        assert_eq!(0, response.left_over_data);
        compare_bytes(expected.as_bytes(), &response.data);
    }

    #[test]
    fn replace_second_timestamp_at_end_of_input() {
        let input = "1530216070";
        let expected = "[2018-06-28 20:01:10 UTC]";
        let mut input_buffer = Vec::new();
        input_buffer.extend_from_slice(&input.as_bytes());
        let response = replace_epoch_timestamps(&input_buffer, true);

        assert_eq!(0, response.left_over_data);
        compare_bytes(expected.as_bytes(), &response.data);
    }

    #[test]
    fn replace_valid_timestamp_with_millisecond_precision_in_place() {
        let input = "prefix1530216070317suffix";
        let expected = "prefix[2018-06-28 20:01:10.317 UTC]suffix";
        let mut input_buffer = Vec::new();
        input_buffer.extend_from_slice(&input.as_bytes());
        let response = replace_epoch_timestamps(&input_buffer, false);

        assert_eq!(0, response.left_over_data);
        compare_bytes(expected.as_bytes(), &response.data);
    }

    #[test]
    fn replace_valid_timestamp_with_second_precision_in_place() {
        let input = "prefix1530216070suffix";
        let expected = "prefix[2018-06-28 20:01:10 UTC]suffix";
        let mut input_buffer = Vec::new();
        input_buffer.extend_from_slice(&input.as_bytes());
        let response = replace_epoch_timestamps(&input_buffer, false);

        assert_eq!(0, response.left_over_data);
        compare_bytes(expected.as_bytes(), &response.data);
    }

    #[test]
    fn replace_multiple_timestamp_with_second_precision() {
        let input = "prefix1530216070middle1530216070suffix";
        let expected = "prefix[2018-06-28 20:01:10 UTC]middle[2018-06-28 20:01:10 UTC]suffix";
        let mut input_buffer = Vec::new();
        input_buffer.extend_from_slice(&input.as_bytes());
        let response = replace_epoch_timestamps(&input_buffer, false);

        assert_eq!(0, response.left_over_data);
        compare_bytes(expected.as_bytes(), &response.data);
    }

    #[test]
    fn replace_multiple_timestamp_with_millisecond_precision() {
        let input = "prefix1530216070317middle1530216070317suffix";
        let expected =
            "prefix[2018-06-28 20:01:10.317 UTC]middle[2018-06-28 20:01:10.317 UTC]suffix";
        let mut input_buffer = Vec::new();
        input_buffer.extend_from_slice(&input.as_bytes());
        let response = replace_epoch_timestamps(&input_buffer, false);

        assert_eq!(0, response.left_over_data);
        compare_bytes(expected.as_bytes(), &response.data);
    }

    #[test]
    fn indicate_trailing_numeric_chars() {
        let input = "prefix15302160";
        let expected = "prefix";
        let mut input_buffer = Vec::new();
        input_buffer.extend_from_slice(&input.as_bytes());
        let response = replace_epoch_timestamps(&input_buffer, false);

        assert_eq!(8, response.left_over_data);
        compare_bytes(expected.as_bytes(), &response.data);
    }

    #[test]
    fn replace_in_file() {
        let mut open_options = OpenOptions::new();
        open_options.write(true).create_new(true).truncate(true);
        let timestamp = format!("{:?}", Instant::now());
        let name: String = "/tmp/".to_string() + &timestamp.to_string();
        let name2: String = "/tmp/".to_string() + &timestamp.to_string();
        let mut test_data_file = open_options.open(&name).unwrap();
        let test_data = "abcdef\nsome1530216070timestamp\nfoo\nprefix1530216070317suffix\nbar\n\n";
        let mut expected: String = String::new();

        for _ in 0..100 {
            test_data_file
                .write(test_data.as_bytes())
                .expect("Failed to write file");
            expected.push_str(&"abcdef\nsome[2018-06-28 20:01:10 UTC]timestamp\nfoo\nprefix[2018-06-28 20:01:10.317 UTC]suffix\nbar\n\n".to_string());
        }
        test_data_file.flush().expect("Failed to flush file");

        process_files(&[name]);
        assert_file_content(name2 + &".depoch".to_string(), expected.as_bytes())
    }

    #[test]
    fn replace_in_file_over_buffer_boundary() {
        let mut open_options = OpenOptions::new();
        open_options.write(true).create_new(true).truncate(true);
        let timestamp = format!("{:?}", Instant::now());
        let name: String = "/tmp/".to_string() + &timestamp.to_string();
        let name2: String = "/tmp/".to_string() + &timestamp.to_string();
        let mut test_data_file = open_options.open(&name).unwrap();
        let mut expected: String = String::new();

        for _ in 0..BUFFER_SIZE - 4 {
            test_data_file
                .write("a".as_bytes())
                .expect("Failed to write file");
            expected.push_str(&"a".to_string());
        }
        test_data_file
            .write("1530216070317".as_bytes())
            .expect("Failed to write file");
        expected.push_str(&"[2018-06-28 20:01:10.317 UTC]".to_string());
        test_data_file.flush().expect("Failed to flush file");

        process_files(&[name]);
        assert_file_content(name2 + &".depoch".to_string(), expected.as_bytes())
    }

    fn assert_file_content(file_name: String, expected: &[u8]) {
        let mut open_options = OpenOptions::new();
        open_options.read(true);
        let mut input_file = open_options.open(file_name).unwrap();
        let mut buffer = Vec::new();
        input_file
            .read_to_end(&mut buffer)
            .expect("Failed to read file");
        compare_bytes_len(expected, &buffer.as_slice(), expected.len());
    }

    fn compare_bytes(a: &[u8], b: &[u8]) {
        assert_eq!(
            a.len(),
            b.len(),
            "Input lengths differ: {}, {}",
            a.len(),
            b.len()
        );

        compare_bytes_len(a, b, b.len())
    }

    fn compare_bytes_len(a: &[u8], b: &[u8], length: usize) {
        for index in 0..length {
            assert_eq!(
                a[index] as char, b[index] as char,
                "Bytes at position {} differ",
                index
            );
        }
    }
}
