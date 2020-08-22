insert_control_info = """
                      INSERT INTO CONTROL_INFO (FILE_NAME, BUCKET_NAME, IS_PROCESSED)
                      values (%s, %s, %s)
                      """

insert_map = {"CONTROL_INFO": insert_control_info}

fetch_control_info = """
                     SELECT FILE_NAME, BUCKET_NAME FROM CONTROL_INFO WHERE IS_PROCESSED = FALSE 
                     ORDER BY CREATED_DATETIME asc;
                     """

fetch_map = {"CONTROL_INFO": fetch_control_info}