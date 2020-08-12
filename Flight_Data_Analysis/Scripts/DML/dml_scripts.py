insert_control_info = """
                      INSERT INTO CONTROL_INFO (FILE_NAME, BUCKET_NAME, IS_PROCESSED)
                      values (%s, %s, %s)
                      """

insert_map = {"CONTROL_INFO": insert_control_info}