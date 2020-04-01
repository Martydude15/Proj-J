"""
    for date_format in POSSIBLE_DATE_FORMATS :
        try: 
            msp_data['Fault Time'] = datetime.strptime(raw_string_date , date_format)
            break
        except ValueError:
            pass
    """
    date_column = 'Fault Time'
    # Parse the dates in each format and stash them in a list
    fixed_dates = [pd.to_datetime(msp_data[date_column], errors='coerce', format=fmt) for fmt in POSSIBLE_DATE_FORMATS]
    
    # Anything we could parse goes back into the CSV
    msp_data[date_column] = pd.NaT
    for fixed in fixed_dates:
        msp_data.loc[~pd.isnull(fixed), date_column] = fixed[~pd.isnull(fixed)]
		
		"""
        msp_data['Fault Time'] = pd.to_datetime(msp_data['ZULU_Time'].str.split(" "
    , n = 2, expand = True)[2], format = '%H:%M:%S:%f', errors='coerce').dt.time  
"""
""" 
    msp_data['Fault Time'] = pd.to_datetime(msp_data['ZULU_Time'].str.split(" "
    , n = 2, expand = True)[2], format = '%H:%M:%S:%f', errors='coerce').dt.time
"""  