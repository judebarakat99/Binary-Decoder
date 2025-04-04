# Q1 Answer: #Number of complete data frames in the file: 748
# Q2 Answer: #Number of corrupt messages: 16
# Q3 Answer: #Calendar dates for corrupt messages: 2006-05-15	2006-05-15	2006-05-15	2006-05-11	2006-05-15	2853-09-23	2006-05-15	2006-05-15	2006-05-15	2006-05-15	year 97909 is out of range	2006-05-15	3379-12-26	2006-05-15	2006-05-15	2005-10-11	

############################################################################################################################################################
import csv
import datetime

# Lookup table for temperature values, changed the values to integers for easier comparison
lookup_table = {
    160: 30,    164: 30.4,  168: 30.8,  172: 31.2,
    176: 31.6,  180: 32,    184: 32.4,  188: 32.8,
    192: 33.2,  196: 33.6,  200: 34,    204: 34.4,
    208: 34.8,  212: 35.2,  216: 35.6,  220: 36,
    161: 30.1,  165: 30.5,  169: 30.9,  173: 31.3,
    177: 31.7,  181: 32.1,  185: 32.5,  189: 32.9,
    193: 33.3,  197: 33.7,  201: 34.1,  205: 34.5,
    209: 34.9,  213: 35.3,  217: 35.7,  221: 36.1,
    162: 30.2,  166: 30.6,  170: 31,    174: 31.4,
    178: 31.8,  182: 32.2,  186: 32.6,  190: 33,
    194: 33.4,  198: 33.8,  202: 34.2,  206: 34.6,
    210: 35,    214: 35.4,  218: 35.8,  222: 36.2,
    163: 30.3,  167: 30.7,  171: 31.1,  175: 31.5,
    179: 31.9,  183: 32.3,  187: 32.7,  191: 33.1,
    195: 33.5,  199: 33.9,  203: 34.3,  207: 34.7,
    211: 35.1,  215: 35.5,  219: 35.9,  223: 36.3
}

def lookup(value):
    """Convert a lookup table integer value to its corresponding hex key.
    
        Parameters
        ----------
        value : int
            An integer value.
        return : int    
    """

    # Iterate over the lookup table and find the corresponding value
    for key, table_value in lookup_table.items():
        # Compare the byte_value with the key in the table
        if key == value:
            return table_value  # Returns the corresponding table value
    
    return 0.0  # Return 0.0 if the value is not found


def convert_int2byte(byte):
    """Convert an integer to a byte.
    
        Parameters
        ----------
        byte : int
            An integer value.
        return : str
    """
    return str(byte)

def combine_to_16(byte1, byte2):
    """Combine two 8-bit bytes into a single 16-bit integer.
    
        Parameters
        ----------
        byte1 : int
            An integer value of the first byte.
        byte2 : int
            An integer value of the second byte.
        return : int
        return : int
        return : int
    """
    # Combine them into a 16-bit integer
    unsigned_value = (byte1 << 8) | byte2
    if unsigned_value < 32768:
        signed_value = unsigned_value 
    else:
        signed_value = unsigned_value - 65536 

    return unsigned_value, unsigned_value, signed_value

def validate_checksum(row, corrupt_errors):
    """Validate the checksum of a row and return the row if valid, otherwise return a corrupt list.
    
        Parameters
        ----------
        row : list
            A list of bytes.
        return : list
    """
    corrupt = False

    if len(row) != 27:
        #corrupt_errors.append(["Invalid row length!", len(row)])
        #corrupt = True
        return corrupt, corrupt_errors
   
    expected_checksum = row[25]  # The checksum value in byte 26

    # Check if the first three bytes are 37, 37, 200
    if row[0] != 37 or row[1] != 37 or row[2] != 200:
        corrupt_errors.append(["Invalid start bytes!", row[0], row[1], row[2]])
        corrupt = True
        return corrupt, corrupt_errors

    # Exclude the two '37' values and the checksum itself
    filtered_data = row[2:25]
    
    # Step 1: Sum all values in the filtered_data
    total_sum = sum(filtered_data)
    
    # Step 2: Take modulo of the sum against 256
    mod_value = total_sum % 256
    
    # Step 3: Subtract the result from 255
    calculated_checksum = 255 - mod_value

    # Step 4: Compare calculated checksum with expected checksum
    if calculated_checksum == expected_checksum:
        return corrupt, corrupt_errors
    else:
        corrupt = True
        corrupt_errors.append(["Invalid checksum!", expected_checksum, calculated_checksum])
        return corrupt, corrupt_errors

    
def find_dates(row):
    """Find the date of a row and return the date.
    
        Parameters
        ----------
        row : list
            A list of bytes.
        return : date
    """
    try:
        timestamp = handle_timing(row)
        date = datetime.datetime.fromtimestamp(timestamp // 1_000_000, datetime.timezone.utc).date() 
        #if timestamp else "Unknown"
    except Exception as e:
        return e
    
    return date

def decode_everything(all_rows):
    """Decode all rows and return the decoded rows and the number of errors.
    
        Parameters
        ----------
        all_rows : list
            A list of all raw rows.
        return : list
        return : int
    """
    print_rows = []
    checksum_errors = []
    checksum_errors_count = 0
    seq_errors_count = 0
    prev_seq=0
    curr_seq=0
    dates = []
    row = all_rows[0]
    prev_seq = row[5]-1

    for row in all_rows:
        curr_seq = row[5]
        try:
            seq_errors_count = check_sequence(prev_seq, curr_seq)
        except:
            None

        checksum = validate_checksum(row, checksum_errors)
        checksum_errors = checksum[1]

        if checksum[0]:
            dates.append(find_dates(row))
            checksum_errors_count += 1
        
        if len(row) == 27:
            print_rows.append(decode_rows(row))
            
        prev_seq = curr_seq
    
    errors = [seq_errors_count , checksum_errors_count]
        
    return print_rows, dates, errors, checksum_errors

def check_sequence(prev, curr, errors):
    """Check the sequence of byte 7 and increment the errors if the sequence is incorrect.
    
        Parameters
        ----------
        byte_7 : int
            An integer value of byte 7.
        errors : int
            An integer value of the errors.
        return : int
    """
    if prev == curr -1:
        return errors
    elif prev == 255 and curr == 0:
        return errors
    else:
        errors += 1
        print ("prev", prev, "curr", curr)
    return errors

def handle_payload(raw_row):
    """Handle the payload of a row and return the payload as a list.
    
        Parameters
        ----------
        raw_row : list
            A list of bytes.
        return : list
    """
    raw_index = 8
    payload = []
    signed_determinant = 0

    for i in range(raw_index, 14, 2):
        #print("bytes are:", raw_row[i], raw_row[i+1])
        new_val = combine_to_16(raw_row[i], raw_row[i+1])
        payload.append(new_val[signed_determinant])
        signed_determinant += 1

    lookup_val = lookup(raw_row[14])
    payload.append(lookup_val)

    lookup_val = lookup(raw_row[15])
    payload.append(lookup_val)

    #print("payload", payload)
    return payload

def bytes_to_timestamp(int_list):
    """Convert 8 bytes into a 64-bit integer timestamp.
    
        Parameters
        ----------
        int_list : list
            A list of 8 integers.

        return : int
    """

    # Check if the input list is empty or has invalid values is alreadyb done in the main function
    # Convert the list of integers to a byte array
    byte_array = b''.join([i.to_bytes(1, byteorder='big', signed=False) for i in int_list])
    
    # Check if the byte array has exactly 8 bytes
    if len(byte_array) != 8:
        #print(f"Input must be exactly 8 bytes. Actual length: {len(byte_array)}")
        None
    
    # Convert the byte array into a 64-bit integer
    return int.from_bytes(byte_array, byteorder='big', signed=False)

def handle_timing(raw_row):
    """Handle the timing of a row and return the timestamp.
    
        Parameters
        ----------
        raw_row : list
            A list of bytes.
        return : int    
    """
    byte_data = raw_row[17:25]
    timestamp = bytes_to_timestamp(byte_data)
    return timestamp

def decode_rows(raw_row): 
    """Decode a raw row and return the decoded row.
    
        Parameters
        ----------
        raw_row : list
            A list of bytes.
        return : list
    """
    print_row = [None] * 15

    payload = handle_payload(raw_row)
    inc_byte = raw_row[5]

    try:
        print_row[0] = "~~"
        print_row[1] = raw_row[3]
        print_row[2] = raw_row[4]
        print_row[3] = raw_row[5]
        print_row[4] = raw_row[6]
        print_row[5] = inc_byte
        print_row[6] = "P"
        print_row[7] = payload[0]
        print_row[8] = payload[1]
        print_row[9] = payload[2]
        print_row[10] = payload[3]
        print_row[11] = payload[4]
        print_row[12] = "T"
        print_row[13] = handle_timing(raw_row)
        print_row[14] = raw_row[25]
    except:
        None

    return print_row

def arrange_rows(file_contents):
    """
    Arrange the rows in the correct order.
    
        Parameters
        ----------
        file_contents : bytes
            A bytes object.
        return : list
    """

    # Convert the bytes object to a list of individual bytes
    bytes_list = list(file_contents)

    # Initialize variables
    all_rows = []
    segment = []

    # Append the first segment if any
    for i in range(0, len(bytes_list)):
        if bytes_list[i] == 37 and bytes_list[i-1] == 37:
            if segment:
                all_rows.append(segment.copy())
            segment = [37, 37,200]
            break
        else:
            segment.append(bytes_list[i])

    # Iterate through the bytes list
    for i in range(0, len(bytes_list)):
        if bytes_list[i] == 37 and bytes_list[i-1] == 37:
            #if len(segment) >= 25:
            if segment:
                all_rows.append(segment.copy())
            segment = []
        elif bytes_list[i] == 200 and bytes_list[i-1] == 37:
            if segment:
                all_rows.append(segment.copy())
            segment = [bytes_list[i-2], 37, 200]
        else:
            segment.append(bytes_list[i])

    # Append the last segment if any
    if segment:
        all_rows.append(segment.copy())

    return all_rows

def print_to_file(AnswerQ1, AnswerQ2, errors, AnswerQ3, checksum_errors):
    """Print the answers to a file.
    
        Parameters
        ----------
        AnswerQ1 : int
            An integer value of the answer to question 1.
        AnswerQ2 : int
            An integer value of the answer to question 2.
        errors : list
            A list of errors.
        AnswerQ3 : set
            A set of dates.
        checksum_errors : list
            A list of checksum errors.
    """
    q1 = "#Number of complete data frames in the file: " + str(AnswerQ1)
    q2 = "#Number of corrupt messages: " + str(AnswerQ2)
    q3 = "#Calendar dates for corrupt messages: "
    for date in AnswerQ3:
        q3 += str(date) + "\t"

    with open(__file__, "r") as f:
        lines = f.readlines()

    with open(__file__, "w") as f:
        for line in lines:
            if line.startswith("# Q1 Answer:"):
                f.write(f"# Q1 Answer: {q1}\n")
            elif line.startswith("# Q2 Answer:"):
                f.write(f"# Q2 Answer: {q2}\n")
            elif line.startswith("# Q3 Answer:"):
                f.write(f"# Q3 Answer: {q3}\n")
            else:
                f.write(line)
    # with open("answers.txt", "w") as file:
    #     file.write("Number of complete data frames in the file: " + str(AnswerQ1) + "\n\n")

    #     file.write("Number of corrupt messages: " + str(AnswerQ2) + "\n")
    #     file.write("Number of sequence errors: " + str(errors[0]) + "\n")
    #     file.write("Number of checksum errors: " + str(errors[1]) + "\n\n")

    #     file.write("Checksum errors: \n")
    #     for error in checksum_errors:
    #         if error:
    #             file.write("\t" + str(error) + "\n")

    #     file.write("\nCalendar dates for corrupt messages: \n" )
    #     for date in AnswerQ3:
    #         file.write("\t" + str(date) + "\n")

def answer_question(complete_frames, corrupt_frames, dates, errors, checksum_errors):
    """Answer the questions based on the data.
    
        Parameters
        ----------
        complete_frames : int
            An integer value of the complete frames.
        corrupt_frames : int
            An integer value of the corrupt frames.
        dates : set
            A set of dates.
    """
    #Q1 How many complete data frames are in your file?  
    AnswerQ1 = corrupt_frames
    print("Number of comptete data frames in the file: ", AnswerQ1)

    #Q2 How many messages might be corrupt? 
    AnswerQ2 = errors[1] +  errors[0]
    print("\nNumber of corrupt messages: ", AnswerQ2)
    print("Number of sequence errors: ", errors[0])
    print("Number of checksum errors: ", errors[1])
    print("Checksum errors: \n", checksum_errors)

    #Q3 What is the calendar date of these messages? 
    AnswerQ3 = dates
    print("\nCalendar dates for corrupt messages: \n", AnswerQ3)

    print_to_file(AnswerQ1, AnswerQ2, errors, AnswerQ3, checksum_errors)

def write_to_CSV(output_csv_path, decoded_rows):
    """Write the rows to a CSV file.
    
        Parameters
        ----------
        output_csv_path : str
            A string value of the output CSV file path.
        decoded_rows : list
            A list of decoded rows.
    """
    with open(output_csv_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(decoded_rows)


# Open the file in read mode
with open("binaryFileC_10.bin", "rb") as file:
    # Read the entire file
    file_contents = file.read()

all_rows = []
all_rows = arrange_rows(file_contents)
#print("Number of rows:", len(all_rows))
decoded_rows, dates, errors, checksum_errors = decode_everything(all_rows)
answer_question(len(all_rows), len(decoded_rows), dates, errors, checksum_errors)

#write_to_CSV("raw_output.csv", all_rows)
write_to_CSV("11477895.csv", decoded_rows)