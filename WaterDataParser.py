#! python3
"""
This program is used to create a water sample database output file in comma-separated-value 
(csv) format, based on input csv files containing lab or field-measured results. 
The program looks in the "For Script" folder for the required file names. 
Upon completion, the input source files are moved to the "Raw Data Archive" folder, 
unless suppressed by using -nfm. The output files are put into the "For Upload" folder.

This program produces:
    1. a .csv file for input into the Access database for each set of input files
    2. a file listing warnings, if any warnings occur

optional arguments:
  -h, --help          show this help message and exit
  -i, --interactive   queries user for instruction on warning conditions (default)
  -a, --auto          run without user queries on warnings
  -nfm, --noFileMove  inhibit removal of source files, for debug
"""   

#
# Stuff to do:
#   set it so it can be run from Automate folder - done
#   set QAQC_Status to Preliminary/Accepted or Preliminary/Rejected - done
#   change to default to interactive mode - done
#   set time and date limits as globals - done

## @details Fetches user input arguments, if any, and sets variables accordingly. 
def ParseArguments():
    global interactive, fileMove

    if len(sys.argv) > 1:
        parser = argparse.ArgumentParser(description = __doc__)

        parser.add_argument("-i","--interactive", action="store_true", help="queries user for instruction on warning conditions")
        parser.add_argument("-a","--auto", action="store_true", help="run without user queries on warnings (default)")
        parser.add_argument("-nfm","--noFileMove", action="store_true", help="inhibit removal of source files, for debug")
        args = parser.parse_args()
        if args.noFileMove :
            fileMove = False
        if args.interactive :
            interactive = True
        if args.auto :
            interactive = False


## @parblock @return Returns a list project input files found, with a dictionary of info for each file.@endparblock
## Scans the folder of data files for filename matches, returns a sorted list to be processed.
## Each item in the list is a dictionary of filename, sample date, and if applicable, another
## associated file.
## For VMM MWRA lab files, the associated file is a "_forscript_VMMtempdepth.csv" file of 
## the same date.
## If the folder for input files "For Script" is not found, the program quits without a warning file.
##    
## Uses global projectCode.
def GetProjectInputFileList() :
    dir = "For Script"
    setPath(dir)
    fileList = []
    flgFiles = []
    dateMatch = '2[0-9][0-9][0-9][01][0-9][0-3][0-9]' # this breaks in the year 3000!
    projectInputMatch = {"FLG":"_forscript_Flagging.csv", "VMM":"_forscript_MWRA.csv", "Field":"_forscript_VMMtempdepth.csv"}
    flgFiles = fnmatch.filter(os.listdir(dir), dateMatch+projectInputMatch[projectCode])
    for file in flgFiles :
        auxFile = ""
        fileDate = GetDateTimeObject(file[0:8]).date()
        filepath = dir+os.sep+file
        if projectCode == "VMM" :
            # see if there is a corresponding Field file, if so, add to list
            auxFile = fnmatch.filter(os.listdir(dir), YearMonthDay(fileDate)+projectInputMatch["Field"])
            if len(auxFile) > 0 :
                auxFile = dir+os.sep+auxFile[0]
        fileList.append({"File":filepath,"Date":fileDate, "Field File":auxFile})
    return(fileList)

## @parblock @param [in] dir Name of folder to seek for input files.
## If the folder is not found, it is looked for in the directory above, and if it is found there,
## the script working directory is set to the directory above (..)
def setPath(dir) :
    dirFound = os.path.exists(dir)
    if not dirFound :
        dirFound = os.path.exists(".."+os.sep+dir)
        if dirFound :
            os.chdir("..")
    if not dirFound :
        print("Did not find data file folder '"+dir+"' - Quitting!")
        exit(1)



## @parblock @param [in] labFile String pathname to the lab data file to get the data from.@endparblock
## Read the lab report file containing the sample data measurements.
##
## Uses global projectCode, fills labData.
def GetLabFileData ( labFile ) :
    
    # By specifying the header names rather than reading them from the file, we avoid bad
    # characters which might occur at the beginning of the file. We also avoid duplicate "Name" columns
    if projectCode == "FLG" :
        labKeys = ["Text Id","Sampled Date","Site Name","Formatted Entry","Temperature", "Depth","Comment","FDUP Site"]
        
    elif projectCode == "VMM":
        labKeys = ["Sample Number","Text Id","Site Name","Description","X Trip","Sampled By","Test Location","Status","Sampled Date","Analyzed On","Analysis","Test Name","Formatted Entry","Display String","Batch","X Result Flags","FDUP Site","X Sample Flags","Test Comment"]
        # ""X Test Flags"" gets mapped to "FDUP Site"
    else:
        print("got unsupported project code: ", projectCode)

    with open(labFile, 'r') as csvfile:
        labfilereader = csv.DictReader(csvfile, fieldnames = labKeys, dialect='excel')
        labHeader = {}
        for row in labfilereader:
            if len(labHeader) == 0:
                labHeader = row # header row not currently used
            else :
                if not row["Site Name"]:
                    # omit empty data rows
                    continue
                if projectCode == "FLG":
                    row["Test Name"] = "FLG E. coli"
                    row["Display String"] = "cfu/100ml"
                labData.append(row)
    csvfile.close()


## @details Based on the data from the lab report file, fill in the fields for access database data. 
##    
## There is a row in the Access file for each row in the lab data file, unless there is no measurement. 
## For each field in each row in the access data, rules are implemented to fill the field from 
## the input data. 
## Dupe info and VMM field comments are filled in elsewhere.
##    
## Uses global projectCode, labData, fills accessData, rovAddresses, siteRows, siteTestRows, dupeSiteRows.
def FillAccessData():

    # Here are the Access file output headings:
    #accessHeadings = ["Activity_ID","Lab_ID","Date_Collected","Time_Collected","Site_ID","Project_ID","Component_ID","Actual_Result","Actual_Result_Unit_ID","Activity_Type_ID","Actual_Result_Type_ID","Result_Sample_Fraction","Reporting_Result","Reporting_Result_Unit_ID","Reporting_Result_Type_ID","Collection_ID","Analytical_Method_ID","Associated_ID","Data_Type_ID","Media_Type_ID","Media_Subdivision_ID","Relative_Depth_ID","Result_Comment","Field_Comment","Event_Comment","QAQC_Comment","Percent_RPD","QAQC_Status"]

    # There is a row in the Access file for each row in the lab data file. Each of the columns
    # in the Access file is implemented as a dictionary key. For each key there is a rule for
    # translating the input data to the output file.
    rowCount = 0
    ltGtFound = False
    for labRow in labData:
        site = GetSiteId(labRow)
        if site != "FDUP" and site not in projectSites[projectCode] :
            response = WarningWithReplace("Found unknown site identifier: "+site+" not in project "+projectCode)
            if response:
                site = response
                labRow["Site Name"] = site
        # cover the condition of the Sample Address, with no measure data
        if labRow["Test Name"] == "Sample Address" :
            # save the Sample Address to put into the Field Comment, in the row with the data
            sampleDateTime = GetSampleDateTime(labRow)
            siteDateKey = site+YearMonthDay(sampleDateTime)
            rovAddresses[siteDateKey] = labRow["Formatted Entry"]
            continue
            
        # otherwise, fill each column in accessHeadings:
        accessDataRow = {}
        activityID = GetActivityId(projectCode, labRow)
        accessDataRow["Activity_ID"] = activityID
        accessDataRow["Lab_ID"] = labRow["Text Id"]
        sampleDateTime = GetSampleDateTime(labRow)
        accessDataRow["Date_Collected"] = AccessFormatDate(sampleDateTime)
        accessDataRow["Time_Collected"] = AccessFormatTime(sampleDateTime)
        accessDataRow["Site_ID"] = site
        siteRows.append(site) # save which sites processed, for later
        if site == "FDUP" :
            # save the dupe row index for later
            if labRow["FDUP Site"] in projectSites[projectCode]:
                dupeSiteRows[rowCount] = labRow["FDUP Site"]
            else:
                dupeSiteRows[rowCount] = "FDUP"
                response = WarningWithReplace("Dupe site "+labRow["FDUP Site"]+" is invalid for project "+projectCode)
                if response :
                    dupeSiteRows[activityID] = response

        accessDataRow["Project_ID"] = projectCodes[projectCode]
        accessDataRow["Component_ID"] = GetAnalysisInfo(labRow)["code"]
        # here we save the row indexed by activityID, for later use
        siteTestRows[activityID] = rowCount
        
        # data and < > rules:
        accessDataRow["Result_Comment"] = ""
        if projectCode == "VMM" and len(labRow["Test Comment"]) > 1 and labRow["Test Comment"] != "nil":
            accessDataRow["Result_Comment"] = labRow["Test Comment"]
        result = labRow["Formatted Entry"]
        accessDataRow["Actual_Result"] = result
        if result.find("<") > -1 and IsNumber(result.strip("<")) :
            accessDataRow["Reporting_Result"] = float(result.strip("<"))/2
            accessDataRow["Result_Comment"] = 'Changed censored value, removed "<" symbol, halved value'
            accessDataRow["Actual_Result_Type_ID"] = resultTypes["Calculated"]
            ltGtFound = True
        elif (result.find(">")) > -1 and IsNumber(result.strip(">")) :
            accessDataRow["Reporting_Result"] = float(result.strip(">"))
            accessDataRow["Result_Comment"] = 'Changed censored value, removed ">" symbol'
            accessDataRow["Actual_Result_Type_ID"] = resultTypes["Calculated"]
            ltGtFound = True
        elif IsNumber(result) :
            accessDataRow["Reporting_Result"] = result
            accessDataRow["Actual_Result_Type_ID"] = resultTypes["Actual"]
        else :
            Warning(accessDataRow["Activity_ID"] + " has invalid Formatted Entry result :"+result)
            continue

        accessDataRow["Actual_Result_Unit_ID"] = unitCodes[labRow["Display String"]]
        accessDataRow["Activity_Type_ID"] = GetActivityType(labRow["Test Name"], site)
        accessDataRow["Result_Sample_Fraction"] = GetAnalysisInfo(labRow)["fraction"]
        accessDataRow["Reporting_Result_Unit_ID"] = accessDataRow["Actual_Result_Unit_ID"]
        accessDataRow["Reporting_Result_Type_ID"] = accessDataRow["Actual_Result_Type_ID"]
        accessDataRow["Collection_ID"] = GetCollectionMethod(labRow["Test Name"], site, siteCollectionExceptions)
        accessDataRow["Analytical_Method_ID"] = GetAnalysisInfo(labRow)["name"]
        accessDataRow["Associated_ID"] = "" # dupe info to be filled in later
        accessDataRow["Data_Type_ID"] = dataTypes["Critical"]
        accessDataRow["Media_Type_ID"] = mediaTypes["Water"]
        accessDataRow["Media_Subdivision_ID"] = mediaSubtypes["Surface Water"]
        accessDataRow["Relative_Depth_ID"] = relativeDepthTypes["Surface"]
        accessDataRow["Field_Comment"] = ""
        if "Comment" in labRow.keys() and len(labRow["Comment"]) > 0 :
            accessDataRow["Field_Comment"] = labRow["Comment"]
        accessDataRow["Event_Comment"] = ""
        accessDataRow["QAQC_Comment"] = "" # dupe info to be filled in later
        accessDataRow["Percent_RPD"] = "" # dupe info to be filled in later
        accessDataRow["QAQC_Status"] = "Preliminary"

        accessData.append(accessDataRow)
        rowCount = rowCount + 1
    return(ltGtFound)


## @parblock @param [in] fieldFile File pathname for the VMM temp & depth file that corresponds to the MWRA lab data file of the same date.@endparblock
## Based on contents of a separate file, fill in the comments fields in the 
## access data. For a given site and a given sample date, if the fieldFile 
## has Field Comment info, it is added to the accessData.
##
## This routine also performs a check that the sample Time_Collected between the lab data and the fieldFile,
## for non-dupe samples, are the same or at least agree within 30 minutes.
##    
## Uses global rovAddresses, modifies accessData.
def FillAccessFieldComments( fieldFile ) :  
    
    if fieldFile :
        siteKey = ""
        commentKey = ""
        dateKey = ""
        noComments = True
        siteComments = {}
        siteTimes = {}
        with open(fieldFile, 'r', newline='') as csvfile:
            commentfilereader = csv.DictReader(csvfile, dialect='excel')
            for row in commentfilereader:
                if siteKey == "" or commentKey == "" or dateKey == "" :
                    for key in row.keys():
                        if key.find("Site") > -1 :
                            siteKey = key
                        elif key.find("Date") > -1 :
                            dateKey = key
                        elif key.find("Comment") > -1 :
                            commentKey = key

                if not row[siteKey] or not row[dateKey] :
                    # omit empty data rows
                    continue
                
                sampleDateTime = GetDateTimeObject(row[dateKey])
                sampleDate = YearMonthDay(sampleDateTime.date())
                dateSiteKey = row[siteKey]+sampleDate
                sampleTime = sampleDateTime.time()
                siteTimes[dateSiteKey] = sampleTime

                if len(row[commentKey]) > 0 :
                    if len(row[siteKey]) >0 :
                        siteComments[dateSiteKey] = row[commentKey]
                        noComments = False
                    elif len(row[siteKey]) > 0 and len(row[commentKey]) > 0 :
                        Warning("site "+row[siteKey]+" has comments, but not found in sampled sites")                
        csvfile.close()
        if noComments :
            print("No comments have been found for any sites in "+fieldFile)
    
    for row in accessData:
        site = row["Site_ID"]
        sampleDateTime = GetDateTimeObject(row["Date_Collected"] + " " + row["Time_Collected"])
        sampleDate = YearMonthDay(sampleDateTime.date())
        dateSiteKey = site+sampleDate
        sampleTime = sampleDateTime.time()
        
        # check for agreement between Time_Collected in the lab data versus the fieldFile
        if dateSiteKey in siteTimes.keys():
            if row["Activity_ID"][-1] == "1" and sampleTime != siteTimes[dateSiteKey] :
                delta = abs(timedelta(hours = sampleTime.hour - (siteTimes[dateSiteKey]).hour, minutes=sampleTime.minute - (siteTimes[dateSiteKey]).minute)).total_seconds()/60.
                if delta > maxTimeDiff :
                    Warning(row["Activity_ID"]+" Time_Collected " +row["Time_Collected"]+ " does not match time found in "+fieldFile+" for site "+site+" field "+dateKey+": "+str(siteTimes[dateSiteKey]))
        
        if dateSiteKey in rovAddresses.keys():
            row["Field_Comment"] = rovAddresses[dateSiteKey]
            
        if dateSiteKey in siteComments.keys():
            if row["Field_Comment"] :
                row["Field_Comment"] = row["Field_Comment"] + "; "+siteComments[dateSiteKey]
            else :
                row["Field_Comment"] = siteComments[dateSiteKey]
                


## @details When there are sample duplicates for a given measurement at a given site, certain values are added 
##    to fields in the output Access data, and certain fields are modified.
##    
## Original site sample row gets:
##        - the dupe Activity_ID name in the Associated_ID field
##        - FDUP in the QAQC_Comment field
##        - calculated Percent_RPD in the Percent_RPD field
##        - Preliminary or Rejected in the QAQC_Status
##
## Dupe sample row gets:
##        - Activity_ID FDUP changed to site name
##        - Site_ID changed from FDUP to actual site
##        - the original site sample Activity_ID name in the Associated_ID field
##        - FDUP in the QAQC_Comment field
##        - Collection_ID from original
##        - Field_Comment from original
##        - calculated Percent_RPD in the Percent_RPD field, same as original
##        - Preliminary or Rejected in the QAQC_Status, same as original
##  
## This routine uses global data dupeSiteRows, siteTestRows, and modifies accessData.
def FillDupeAccessData():
    for dupeRow in dupeSiteRows.keys():
        site = dupeSiteRows[dupeRow]
        # swap out FDUP for the true site name
        renamedActivity = (accessData[dupeRow]["Activity_ID"]).replace("FDUP", site, 1)
        # get the activity ID of the original sample, by changing the last char to 1
        origActivity = renamedActivity[:-1]+"1"
    
        if origActivity not in siteTestRows.keys() :
            Warning("No original sample found for activity ID "+renamedActivity + " dupe test, skipping")
            continue
        origRow = siteTestRows[origActivity]
        
        accessData[dupeRow]["Activity_ID"] = renamedActivity
        accessData[origRow]["Associated_ID"] = renamedActivity
        accessData[dupeRow]["Associated_ID"] = accessData[origRow]["Activity_ID"]
        accessData[dupeRow]["Site_ID"] = site
        accessData[dupeRow]["Collection_ID"] = accessData[origRow]["Collection_ID"]
        accessData[dupeRow]["Field_Comment"] = accessData[origRow]["Field_Comment"]
        accessData[origRow]["QAQC_Comment"] = "FDUP"
        accessData[dupeRow]["QAQC_Comment"] = "FDUP"
        # figure out whether to reject
        origMeas = float(accessData[origRow]["Reporting_Result"])
        dupeMeas = float(accessData[dupeRow]["Reporting_Result"])
        test = TestDupeMeasures(origMeas, dupeMeas, accessData[origRow]["Component_ID"])
        percent = test["percent"]
        reportPct = '{:3.2f}'.format(percent)
        status = test["status"]
        
        accessData[origRow]["Percent_RPD"] = reportPct
        accessData[origRow]["QAQC_Status"] = status
        accessData[dupeRow]["Percent_RPD"] = reportPct
        accessData[dupeRow]["QAQC_Status"] = status        


## @parblock @param [in] a,b Sample and sample duplicate measured values
## @return Percentage difference between the two values.@endparblock
## Calculate the percentage.
def CalculatePercent(a, b):
    return(100*(abs(a-b)/(abs(a+b)/2)))


## @parblock @param [in] origMeas Measure of original sample
## @param [in] dupeMeas Measure of duplicate sample
## @param [in] componentID Code of measurement
## @return Dictionary with percentage and status Preliminary/Accepted or Preliminary/Rejected.@endparblock
## For duplicate samples, compute the percentage difference between values, and test
## for Rejected status. Test limits from all nutrient tests come from maxRPDTestLimits.
##
## Uses global maxRPDTestLimits.
def TestDupeMeasures(origMeas, dupeMeas, componentID):
    percentage = CalculatePercent(origMeas, dupeMeas)
    status = "Preliminary/Accepted"
    maxDiff = maxRPDTestLimits[componentID]["diff"]
    maxPercent = maxRPDTestLimits[componentID]["percent"]
    if percentage > maxPercent and abs(origMeas - dupeMeas) > maxDiff :
            status = "Preliminary/Rejected"
    return({"percent":percentage, "status": status})


## @parblock @param [in] rowData Dictionary of one row of sample lab data
## @return String site identifier for the sample.@endparblock
## Finds the site id from the lab data.
def GetSiteId(rowData):
    site_info = rowData["Site Name"].split("-")
    return(site_info[-1])

## @parblock @param [in] rowData Dictionary of one row of sample lab data
## @return datetime object constructed from the sample date and time.@endparblock
##  Lab data for the sample time is of the form mo/day/yr hr:min:00
##  This routine returns a datetime object constructed based on that format.
def GetSampleDateTime(rowData):
    return(GetDateTimeObject(rowData["Sampled Date"]))

## @parblock @param [in] rowData Dictionary of one row of sample lab data
## @return Dictionary of analysisCodes info for the specified test type.@endparblock
## Returns the analysisCode dictionary of analysis test information based on the rowData Test Name.
def GetAnalysisInfo(rowData):
    return(analysisCodes[rowData["Test Name"]])
    
## @parblock @param [in] projectCode Project code string, such as FLG, VMM, etc.
## @param [in] rowData Dictionary of one row of sample lab data
## @return String Activity Identifier@endparblock
##  The Activity ID field is a concatenation of the project abbreviation, the date, the site, 
##  the test performed, and the sample count. 
def GetActivityId(projectCode, rowData):
    sampleDate = GetSampleDateTime(rowData).date()
    site = GetSiteId(rowData)
    abbr = GetAnalysisInfo(rowData)["abbrev"]
    count = "01"
    if site == "FDUP":
        # need to get the actual site id elsewhere in this case
        count = "02"
    return(projectCode + YearMonthDay(sampleDate) + site + abbr + count)


## @parblock @param [in] measure String for the type of measurement
## @param [in] site String site identifier
## @return Activity type code@endparblock
## Returns the activity type code.
def GetActivityType(measure, site):
    if measure == "Depth" or measure == "Temperature":
        return(activityCodes["Field Msr/Obs"])
    if site == "FDUP":
        return(activityCodes["Quality Control Sample-Field Replicate"])
    return(activityCodes["Sample-Routine"])


## @parblock @param [in] measure String for the type of measurement
## @param [in] site String site identifier
## @param [in] siteCollections Dictionary of non-default collection methods by site
## @return Collection method string @endparblock
## Returns the colection method, C-BABR by default.
def GetCollectionMethod(measure, site, siteCollections):
    if measure == "Depth":
        return("N-DL")
    if site in siteCollections.keys():
        method = siteCollections[site]
    else:
        method = "C-BABR"
    if measure == "Temperature":
        method = method.replace("C-", "N-")
    return(method)


## @parblock @param [in] filename File pathname, only used for VMM depth and temperature data.@endparblock
## Add depth and temperature data to the access data. This routine is skipped for VMM data,
## because it is covered separately using the Field pseudo-project. The Field sample file is opened and read
## here. The field sample file may be either from a template, or a copy-paste from Survey123.
## For Flagging, the previously read labData contains the depth and temperature information.
##    
## Uses global projectCode, labData, siteRows, adds to accessData.
def GetDepthAndTemp(filename) :
    if projectCode == "Field":
        # We use the keys found in the data file, but it could be either of two varieties, depending on whether
        # it is from the Survey123 spreadsheet.
        prj = "VMM"
        siteKey = ""
        depthKey = ""
        tempKey = ""
        dateKey = ""
        commentKey = ""
        VMMdata = []
        
        with open(filename, 'r', newline='') as csvfile:
            measureFilereader = csv.DictReader(csvfile, dialect='excel')
            for row in measureFilereader:
                if not siteKey :
                    for key in row.keys():
                        if key.find("Site") > -1 :
                            siteKey = key
                if row[siteKey]:
                    # only save non-empty rows
                    VMMdata.append(row)
            csvfile.close()
        
        for key in VMMdata[0].keys():
            # Here we find the field keys from the input file by finding matches to the column names
            if key.find("Date") > -1 :
                dateKey = key
            elif key.find("Depth") > -1 and key.find("ft") > -1:
                depthKey = key
            elif key.find("Temp") > -1 :
                tempKey = key
            elif key.find("Comment") > -1 :
                commentKey = key
                
        dataRows = VMMdata
    else :
        prj = projectCode
        dataRows = labData
        siteKey = "Site Name"
        depthKey = "Depth"
        tempKey = "Temperature"
        dateKey = "Sampled Date"
        commentKey = "Comment"
        #labKeys = ["Text Id","Sampled Date","Site Name","Formatted Entry","Temperature", "Depth","Comment","FDUP Site"]

    for measure in ["Depth","Temperature"]:
        if measure =="Depth":
            unit = "ft"
            measKey = depthKey
        else:
            unit = "deg C"
            measKey = tempKey
        
        for row in dataRows:
            if not row[siteKey] or row[siteKey] == "FDUP" or not IsNumber(row[measKey]):
                continue
            else:
                site = row[siteKey]
            if projectCode == "FLG" and site not in siteRows:
                Warning("Found" + measure+ "data for site"+ site+ "without lab sample data.")

            measDataRow = {}
            
            sampleDateTime = GetDateTimeObject(row[dateKey])
            measDataRow["Date_Collected"] = AccessFormatDate(sampleDateTime)
            measDataRow["Time_Collected"] = AccessFormatTime(sampleDateTime)
            measDataRow["Site_ID"] = site
            measDataRow["Project_ID"] = projectCodes[projectCode]
            measDataRow["Activity_ID"] = prj + YearMonthDay(sampleDateTime) + site + analysisCodes[measure]["abbrev"] + "01"
            measDataRow["Field_Comment"] = row[commentKey]

            measDataRow["Media_Type_ID"] = mediaTypes["Water"]
            measDataRow["Media_Subdivision_ID"] = mediaSubtypes["Surface Water"]
            measDataRow["Relative_Depth_ID"] = relativeDepthTypes["Surface"]
            measDataRow["Lab_ID"] = "None"
            measDataRow["Component_ID"] = analysisCodes[measure]["code"]
            measDataRow["Actual_Result"] = row[measKey]
            measDataRow["Actual_Result_Unit_ID"] = unitCodes[unit]
            measDataRow["Activity_Type_ID"] = GetActivityType(measure, site) # Field Msr/Obs
            measDataRow["Actual_Result_Type_ID"] = resultTypes["Actual"]
            measDataRow["Result_Sample_Fraction"] = "N/A"
            measDataRow["Reporting_Result"] = measDataRow["Actual_Result"]
            measDataRow["Reporting_Result_Unit_ID"] = measDataRow["Actual_Result_Unit_ID"]
            measDataRow["Reporting_Result_Type_ID"] = measDataRow["Actual_Result_Type_ID"]
            measDataRow["Collection_ID"] = GetCollectionMethod(measure, site, siteCollectionExceptions)
            measDataRow["Analytical_Method_ID"] = analysisCodes[measure]["name"]
            measDataRow["Data_Type_ID"] = dataTypes["Non-critical"]

            #"Associated_ID","Result_Comment",","Event_Comment","QAQC_Comment",and "Percent_RPD" are all null
            for key in ["Associated_ID","Result_Comment","Event_Comment","QAQC_Comment", "Percent_RPD"]:
                measDataRow[key] = ""
            measDataRow["QAQC_Status"] = "Preliminary"
            accessData.append(measDataRow)

# Routines interpreting and formatting dates and times

## @parblock @param [in] timeStr Date or date and time as a string in one of several supported formats
## @return Returns the datetime object for the given date and time.@endparblock
## Tries to make a datetime object from the date-and-time string entered. These types are supported:
##        - 1/21/2020 6:00
##        - 1/21/20 6:00
##        - 1/21/2020 6:00:00 AM
##        - Jan 21, 2020, 6:00 AM (as from Survey123)
##        - Jan 21, 2020, 6:00
##        - 20200121
##        - 1/21/2020
##
## Other variants, including these but with different spacing, are not supported.
def GetDateTimeObject(timeStr) :
    try :
        dt = datetime.strptime(timeStr, "%m/%d/%Y %I:%M") # 1/21/2020 6:00
    except :
        try :
            dt = datetime.strptime(timeStr, "%m/%d/%Y %I:%M:%S %p") # 1/21/2020 6:00:00 AM
        except :
            try :
                dt = datetime.strptime(timeStr, "%m/%d/%y %I:%M") # 1/21/20 6:00
            except :
                try :
                    dt = datetime.strptime(timeStr, "%b %d, %Y, %I:%M %p") # Jan 21, 2020, 6:00 AM
                except :
                    try :
                        dt = datetime.strptime(timeStr, "%b %d, %Y, %I:%M") # Jan 21, 2020, 6:00
                    except :
                        try :
                            dt = datetime.strptime(timeStr, "%Y%m%d") # 20200121
                        except :
                            try :
                                dt = datetime.strptime(timeStr, "%m/%d/%Y") # 1/21/2020
                            except :
                                Warning("Unable to create datetime object from '"+timeStr+"'")
                                return("")
    return(dt)


## @parblock @param [in] dateObj datetime object for a given date and time
## @return Returns the date as a string in the format YYYYMMDD @endparblock
## From a datetime object, returns string with date as YYYYMMDD.
def YearMonthDay(dateObj) :
    return(dateObj.strftime("%Y%m%d"))

## @parblock @param [in] dateTimeObj datetime object for the sample date and time
## @return Returns the date as a string in the format used by Access.@endparblock
## From a datetime object, returns the date in the format used by Access.
def AccessFormatDate(dateTimeObj) :
    return(dateTimeObj.strftime("%m/%d/%Y"))

## @parblock @param [in] dateTimeObj datetime object for the sample date and time
## @return Returns the time as a string in the format used by Access.@endparblock
## From a datetime object, returns the time in the format used by Access.
def AccessFormatTime(dateTimeObj) :
    return(dateTimeObj.strftime("%I:%M:00 %p"))


## @details If a set of access data contains a > greater-than or a < less-than symbol in the
## lab data "Actual_Result" field, the symbols may not be imported into the Access database
## correctly. The work-around to this is to have a row with the symbol be the first row
## imported. This routine finds a row which has the symbol and moves it to the first row
## in the accessData list.
##
## Modifies global accessData
def MoveLtGtRowToTop():
    rowCount = 0
    for row in accessData :
        result = row["Actual_Result"]
        if result.find("<") > -1 or result.find(">") > -1 :
            break
        rowCount = rowCount + 1
        
    substitute = accessData.pop(rowCount)
    accessData.insert(0, substitute)


## @parblock @param [in] fileDate datetime date object for the date that's part of the input file name.@endparblock
## Test the Access data fields for compliance to the template checks.
## Issue warnings for noncompliances found.
##    
## Uses global projectCode, accessData.
def SanityChecks(fileDate):
    # accessHeadings = ["Activity_ID","Lab_ID","Date_Collected","Time_Collected","Site_ID","Project_ID","Component_ID","Actual_Result","Actual_Result_Unit_ID","Activity_Type_ID","Actual_Result_Type_ID","Result_Sample_Fraction","Reporting_Result","Reporting_Result_Unit_ID","Reporting_Result_Type_ID","Collection_ID","Analytical_Method_ID","Associated_ID","Data_Type_ID","Media_Type_ID","Media_Subdivision_ID","Relative_Depth_ID","Result_Comment","Field_Comment","Event_Comment","QAQC_Comment","Percent_RPD","QAQC_Status"]
    
    legalAnalysisCodes = []
    legalTestAbbrev = []
    legalMethods = []
    legalFractions = []
    legalLimits = {}
    for key in analysisCodes.keys() :
        legalTestAbbrev.append(analysisCodes[key]["abbrev"])
        legalAnalysisCodes.append(analysisCodes[key]["code"])
        legalMethods.append(analysisCodes[key]["name"])
        legalFractions.append(analysisCodes[key]["fraction"])
        legalLimits[analysisCodes[key]["code"]] = {"test":key, "lower":analysisCodes[key]["lower"], "upper":analysisCodes[key]["upper"]}
        
    legalUnits = []
    for unit in unitCodes.keys() :
        legalUnits.append(unitCodes[unit])
    legalActivities = []
    for key in activityCodes :
        legalActivities.append(activityCodes[key])
    
    legalCollects = ["C-BABR","C-SPBR", "C-MGW", "C-SPBN", "C-BABN", "C-MGBO", "N-DL","N-BABR","N-SPBR", "N-MGW", "N-SPBN", "N-BABN", "N-MGBO"]
    
    prj = projectCode
    if projectCode == "Field":
        prj = "VMM"
    activityIds = []
    for row in accessData :
        activityIds.append(row["Activity_ID"])
    
    for row in accessData :
        field = "Activity_ID"
        id = row[field]
        if len(id) < 13 or len(id) > 22 or not id.startswith(prj) or id.replace(" ","") != id or id.find("FDUP") > -1 :
            Warning(field + " error: '"+id+"'")
        field = "Lab_ID"
        id = row[field]
        if len(id) < 4 :
            Warning(field + " error, suspiciously short: "+id)
        field = "Date_Collected"
        site = row["Site_ID"]
        sampleDate = GetDateTimeObject(row[field]).date()
        deltaTime = sampleDate - fileDate # deltatime object is returned from date - date
        if abs(deltaTime.days) > maxDateDiff :
            Warning("Site "+site+"_"+field + " error: "+row[field]+" not near to file date "+AccessFormatDate(fileDate))
        field = "Site_ID"
        if site not in projectSites[projectCode] :
            Warning(field + " field error: "+site+ " not legal for "+prj)
        field = "Project_ID"
        id = row[field]
        if projectCodes[projectCode] != id :
            Warning("Site "+site+" "+field + " field error: "+id)
        field = "Component_ID"
        id = row[field]
        cid = id
        if id not in legalAnalysisCodes :
            Warning("Site "+site+" "+field + " field error: "+id)
        for field in ["Actual_Result", "Reporting_Result"] :
            id = row[field]
            if len(str(id)) < 1 :
                Warning("Site "+site+" "+field + " field error: cannot be empty")
        field = "Reporting_Result"
        id = row[field]
        if not IsNumber(str(id)) :
            if row["QAQC_Comment"] != "FDUP" :
                response = WarningWithReplace("Site "+site+" "+field + " field error: "+str(id)+" is not a number")
                if response :
                    row[field] = response
            else :
                Warning("Dupe site "+site+" "+field + " field error: "+str(id)+" is not a number")
        elif float(id) < legalLimits[cid]["lower"] or float(id) > legalLimits[cid]["upper"] :
            if row["QAQC_Comment"] != "FDUP" :
                response = WarningWithReplace("Site "+site+" measured "+legalLimits[cid]["test"] +" outside expected limits: "+str(id))
                if response :
                    row[field] = response
            else:
                Warning("Dupe site "+site+" measured "+legalLimits[cid]["test"] +" outside expected limits: "+str(id))
        for field in ["Actual_Result_Unit_ID", "Reporting_Result_Unit_ID"] :
            id = row[field]
            if id not in legalUnits :
                Warning("Site "+site+" "+field + " field error: "+id)
        field = "Activity_Type_ID"
        id = row[field]
        if id not in legalActivities :
            Warning(field + " field error: "+id)
        for field in ["Actual_Result_Type_ID", "Reporting_Result_Type_ID","Data_Type_ID","Media_Type_ID", "Relative_Depth_ID"] :
            id = row[field]
            if id not in [1,2] :
                Warning("Site "+site+" "+field + " field error: "+id+" not 1 or 2")
        field = "Result_Sample_Fraction"
        id = row[field]
        if id not in legalFractions :
            Warning("Site "+site+" "+field + " field error: "+id)
        field = "Collection_ID"
        id = row[field]
        if id not in legalCollects :
            Warning("Site "+site+" "+field + " field error: "+id)
        field = "Analytical_Method_ID"
        id = row[field]
        if id not in legalMethods :
            Warning("Site "+site+" "+field + " field error: "+id)
        field = "Associated_ID"
        id = row[field]
        if len(id) :
            # should have dupe name
            if id not in activityIds or (row["Activity_ID"][-1] == "1" and row["Activity_ID"][:-1]+"2" != id) or (row["Activity_ID"][-1] == "2" and row["Activity_ID"][:-1]+"1" != id) :
                Warning("Site "+site+" "+field + " field error: "+id)
        field = "Media_Subdivision_ID"
        id = row[field]
        if id != 21 :
            Warning("Site "+site+" "+field + " field error: "+id)
        field = "Result_Comment"
        id = row[field]
        if len(id) > 0 and (id.find("Changed censored value,") < 0 or (row["Actual_Result"][0] != "<" and row["Actual_Result"][0] != ">")) :
            Warning("Site "+site+" "+field + " field error: "+id)
        field = "Event_Comment"
        id = row[field]
        if len(id) > 0 :
            Warning("Site "+site+" "+field + " field error: "+id)
        field = "QAQC_Comment"
        id = row[field]
        if len(id) > 0 and id != "FDUP" :
            Warning("Site "+site+" "+field + " field error: "+id)
        elif id == "FDUP" :
            if not IsNumber(row["Percent_RPD"]) or len(row["Associated_ID"]) < 13 :
                Warning("Site "+site+" Dupe fields Percent_RPD and/or Associated_ID have incorrect info")
        field = "Percent_RPD"
        id = row[field]
        if len(id) > 0 and not IsNumber(str(id)) :
            Warning("Site "+site+" "+field + " field error: "+id)
        field = "QAQC_Status"
        id = row[field]
        if not id in ["Preliminary", "Preliminary/Rejected", "Preliminary/Accepted"] :
            Warning("Site "+site+" "+field + " field error: "+id)


## @parblock @param [in] s String that could represent a number, including a float number
## @return Returns true if it's a number, false otherwise.@endparblock
## Returns True if string is a number, including a float number.
def IsNumber(s) :
    try :
        f = float(s)
    except :
        return(False)
    return(True)


# Routines used for user warnings

## @parblock @param [in] message Warning message string
## @return Replacement value if any, or empty string.@endparblock
## Some warnings may be able to be fixed by the user. The user is prompted to enter a
## replacement value, ignore the warning, or stop the program.
def WarningWithReplace(message):
    PrintWarning(message)
    if interactive :
        print("    Enter a replacement value to continue")
        print("    Enter [cr] to ignore and continue")
        print("    Enter Q to stop")
        answerString= input ()
        if answerString== "" :
            warningFile.write("Warning ignored per user response.\n")
            print ( "Resuming ...\n")        
            return()
        elif answerString[0] == "q" or answerString[0] == "Q" :
            warningFile.write("Quitting per user response to warning.\n")
            print ( "Quitting ...\n")        
            exit(1)
        else :
            warningFile.write("Value replaced with:"+answerString+", per user response to warning.\n")
            print ( "Resuming ...\n")
            return(answerString)    
    
    
## @parblock @param [in] message Warning message string.@endparblock
## In the event of a warning, print the warning to the screen and to the warnings
## file. If interactive mode is in use, ask the user to ignore the warning or quit 
## running the program. 
def Warning(message):
    PrintWarning(message)
    if interactive :
        print("    Enter [cr] to ignore and continue running the program")
        print("    Enter Q to stop the program")
        answerString = input ()
        if answerString == "" :
            warningFile.write("Warning ignored per user response.\n")
            print ( "Resuming ...\n")        
            return()
        elif answerString[0] == "q" or answerString[0] == "Q" :
            warningFile.write("Quitting per user response to warning.\n")
            print ( "Quitting ...\n")        
            exit(1)
        
## @parblock @param [in] message Warning message string@endparblock
## Issue warnings about anomalies found in the data, also write them to a file.
def PrintWarning(message):
    global warningCount, warningFile
    print("Warning:", message)
    if warningFile == sys.stdout :
        filename = "."+os.sep+"For Script"+os.sep+"Warnings_"+YearMonthDay(sampleDate)+"_"+projectDataFiles[projectCode]+".txt"
        warningFile = open (filename, "w")
    warningFile.write(message+"\n")
    warningCount = warningCount + 1

## @details Closes the warning file if it has been opened.
def CloseWarning() :
    global warningFile
    if warningFile != sys.stdout :
        warningFile.close()
        warningFile = sys.stdout


## @parblock @param [in] projectFile String part of file name that is project-specific
## @param [in] formattedDate Date string as YYYYMMDD, from input file name @endparblock
##  Write the collected access data to a .csv file.
##    For Upload - output folder for the script; contains 
##        - YYYYMMDD_forupload_MWRA.csv
##        - YYYYMMDD_forupload_VMMtempdepth.csv
##        - YYYYMMDD_forupload_Flagging.csv
##        - Uploaded Archive - Folder to manually move the uploaded files into when uploading is done         
##    
##  Uses global accessHeadings, accessData
def WriteAccessDataFile(projectFile, formattedDate):
    MakeDirIfNeeded(".", "For Upload")
    with open("For Upload" + os.sep + formattedDate+"_forupload_"+projectFile+".csv", 'w', newline='') as csvfile:
        accessFileWriter = csv.DictWriter(csvfile, fieldnames=accessHeadings, quoting=csv.QUOTE_NONNUMERIC)
        accessFileWriter.writeheader()
        for row in accessData:
            accessFileWriter.writerow(row)
    csvfile.close()


## @parblock @param [in] path path to containing folder
## @param [in] dirName containing folder name to make @endparblock
## Check for existence of folder dirName in path, create if missing.
def MakeDirIfNeeded(path, dirName) :
    for entry in os.scandir(path):
        if entry.name == dirName and entry.is_dir():
            return()
    os.mkdir(path+os.sep+dirName)


## @parblock @param [in] dataFile file to move
## @param [in] path path to containing folder
## @param [in] dirName containing folder name @endparblock
## Check for existence of folder dirname in path, create if missing. 
## Then move the dataFile into the folder.
def MoveCompletedFile(dataFile, path, dirName):
    MakeDirIfNeeded(path, dirName)
    fileName = dataFile.split(os.sep)[-1]
    shutil.move(dataFile, path+os.sep+dirName+os.sep+fileName)

        

#  ############################################-
#  program main
#  ############################################-


import sys, os.path, argparse, csv, time, shutil, fnmatch
from datetime import datetime, timedelta

## Save start time
start_time = time.time()

## Integer counts warnings
warningCount = 0
## Global handle to current warnings file
warningFile = sys.stdout
## Boolean true causes input files to move to archive when done with no warnings
fileMove = True
## Integer keeps track of data points saved to output files
recordCount = 0
## Boolean to make sure some files were found to process
noFilesFound = True
## Boolean true if interactive mode, which asks user to resolve warnings
interactive = True

ParseArguments()

## Dictionary of project codes keyed by project name. "Field" is a pseudo-project, used in the case of VMM sampler data without lab data.
projectCodes = {"CYN":1, "FLG":3, "VMM":7, "Field":7}

## Dictionary containing the partial file names to use in naming input files
projectDataFiles = {"VMM":"MWRA", "FLG":"Flagging", "Field":"VMMtempdepth"}

## Dictionary of activity codes keyed by name
activityCodes = {"Sample-Routine":6, "Quality Control Sample-Field Replicate":5, "Field Msr/Obs":1}

## Dictionary of result type codes keyed by name
resultTypes = {"Actual":1, "Calculated":2}

## Dictionary of unit codes keyed by name
unitCodes = {"MPN/100ml":10, "MPN/100 mL":10, "ug/L":13, "mg/L":7, "deg C":4, "ft":5, "cfu/100ml":3}

## Dictionary of analyses indexed by name, with abbreviation, component code, test name, sample fraction, lower and upper limit
analysisCodes ={"E. coli":{"abbrev":"EC", "code":12, "name":"MWRA-EC-2012", "fraction":"Total", "lower":0.0, "upper":30000.0},
               "Chlorophyll A":{"abbrev":"CA", "code":6, "name":"MWRA-ChlorA-2012", "fraction":"Total", "lower":0.0, "upper":200.0},
               "Phaeophytin":{"abbrev":"PP", "code":20, "name":"MWRA-O&G-2012", "fraction":"Total", "lower":0.0, "upper":20.0},
               "PO4-P":{"abbrev":"OP", "code":18, "name":"MWRA-OPD-2012", "fraction":"Dissolved", "lower":0.0, "upper":0.08},
               "NO32-N":{"abbrev":"NN", "code":14, "name":"MWRA-N/N-2012", "fraction":"Total", "lower":0.0, "upper":10.0},
               "NH3-N":{"abbrev":"NH3", "code":2, "name":"MWRA-NH3D-2012", "fraction":"Dissolved", "lower":0.0, "upper":0.6},
               "Enterococci":{"abbrev":"ENT", "code":11, "name":"MWRA-Ent-2012", "fraction":"Total", "lower":0.0, "upper":20000.0},
               "TN":{"abbrev":"TN", "code":17, "name":"MWRA-TN-2012", "fraction":"Total", "lower":0.0, "upper":15.0},
               "TP":{"abbrev":"TP", "code":21, "name":"MWRA-TP-2012", "fraction":"Total", "lower":0.0, "upper":8.0},
               "TSS":{"abbrev":"TSS", "code":27, "name":"MWRA-TSS-2012", "fraction":"Total", "lower":0.0, "upper":100.0},
               "Depth":{"abbrev":"DTH", "code":8, "name":"Field-Depth-2012", "fraction":"N/A", "lower":0.25, "upper":35.0},
               "Temperature":{"abbrev":"Temp", "code":26, "name":"Therm-Temp-2012", "fraction":"N/A", "lower":-1.0, "upper":45.0},
               "FLG E. coli":{"abbrev":"EC",  "code":12,  "name":"G&L-EC-2012", "fraction":"Total", "lower":0.0, "upper":30000.0}}

## Maximum time difference, in minutes, allowed between reported Collection_Time values for a given site and date.
## Times that are larger produce a warning.
maxTimeDiff = 30.0

## Maximum time difference, in days, allowed between reported Collection_date values and the date in the input file name.
## Date differences that are larger produce a warning.
maxDateDiff = 22.0

## This set of limits determines whether a Percent RPD calculated between duplicate samples is too large, by setting 
## a maximum difference between values and a maximum percentage. Exceeding both limits marks the samples as Rejected.
## When the percent RPD is calculated between duplicate samples, large percentage differences can be reported 
## between values that are small.  There is a set of limits per analysis type.
maxRPDTestLimits = {11:{"diff":100,"percent":100}, # Enterococci
                    12:{"diff":100,"percent":100}, # E. coli
                     6:{"diff":0.0,"percent":100}, # Chlorophyll AS
                    20:{"diff":0.0,"percent":100}, # Phaeophytin
                    18:{"diff":0.0,"percent":100}, # PO4-P
                    14:{"diff":0.0,"percent":100}, # NO32-N
                     2:{"diff":0.0,"percent":100}, # NH3-N
                    17:{"diff":0.0,"percent":100}, # TN
                    21:{"diff":0.0,"percent":100}, # TP
                    27:{"diff":0.0,"percent":100}  # TSS
                   } 

## Dictionary giving tuples of site names legal per project, keyed by project name
projectSites = {"VMM":("35CS","59CS","90CS","130S","165S","199S","229S","267S","269T","290S","318S","343S","387S","400S","447S","484S","521S","534S","567S","591S","609S","621S","635S","648S","662S","675S","012S","700S","715S","729S","743S","760T","763S","773S","784S","QC","ROV1","ROV2"),
"FLG":("1NBS","2LARZ","3BU","4LONG"), "CYN": ("BROAD", "SP", "CB", "ND")}
projectSites["Field"] = projectSites["VMM"]

## Site collection type at each site uses "C-BABR" except these exception sites
siteCollectionExceptions = {"35CS":"C-SPBR", "199S":"C-MGW", "267S":"C-SPBN", "447S":"C-SPBN", "635S":"C-BABN", "648S":"C-MGBO", "1NBS":"C-MGBO", "2LARZ":"C-MGBO", "3BU":"C-MGBO", "4LONG":"C-MGBO"}

## Dictionary of data type codes keyed by name
dataTypes = {"Critical":1, "Non-critical":2, "Unknown":3}

## Dictionary of media types keyed by name, only Water is used.
mediaTypes = {"Water":1, "Air":2, "Biological":3, "Habitat":4, "Sediment":5, "Soil":6, "Tissue":7, "Other":8}

## Dictionary of media subtypes keyed by name - only "Surface Water" is used
mediaSubtypes = {"Surface Water":21}

## Dictionary of relative depth types keyed by name - only "Surface" is used
relativeDepthTypes = {"Surface":1, "Midwater":2, "Near Bottom":3, "Bottom":4, "Subbottom":5}

## Tuple listing the Access file output headings
accessHeadings = ("Activity_ID","Lab_ID","Date_Collected","Time_Collected","Site_ID","Project_ID","Component_ID","Actual_Result","Actual_Result_Unit_ID","Activity_Type_ID","Actual_Result_Type_ID","Result_Sample_Fraction","Reporting_Result","Reporting_Result_Unit_ID","Reporting_Result_Type_ID","Collection_ID","Analytical_Method_ID","Associated_ID","Data_Type_ID","Media_Type_ID","Media_Subdivision_ID","Relative_Depth_ID","Result_Comment","Field_Comment","Event_Comment","QAQC_Comment","Percent_RPD","QAQC_Status")

for projectCode in ["FLG", "VMM", "Field"]:
    
    ## List of files to process for this project, each file is in a tuple of info
    fileList = GetProjectInputFileList()
    
    if len(fileList) > 0 :
        noFilesFound = False
        for processFileInfo in fileList:
            ## File name to process for data
            inputFile = processFileInfo["File"]
            
            ## Sample datetime date object from input filename
            sampleDate = processFileInfo["Date"]
            
            ## Auxilliary file used for VMM site comments, empty except for VMM
            fieldFile = processFileInfo["Field File"]
            
            if projectCode == "VMM" and not fieldFile :
                Warning(projectDataFiles["Field"]+" file not found to go with "+inputFile+"; no field comments available.")
            
            ## This list of dictionaries contains the data to output. The output data is populated from the 
            ## input data per rules coded in FillAccessData(), FillAccessFieldComments(), and FillDupeAccessData().
            accessData = []

            ## Keep track of sites processed
            siteRows = []
        
            ## This list of dictionaries contains the data from the input file.
            labData = []
             
            if projectCode != "Field" :
                GetLabFileData(inputFile)
                
                ## Dictionary keeps track of sample address from lab file for ROV sites
                rovAddresses = {}
                ## Dictionary of rows of access data, keyed by activityID
                siteTestRows = {}
                ## Dictionary of which dupe sites are on which rows of access data
                dupeSiteRows = {}

                # fill all the Access data except field comments and duplicates
                ## Boolean asserts if < or > was found in any of the "Actual_Result" fields
                ltGtFound = FillAccessData()

                # Here is where we update the data for the sample duplicates
                FillDupeAccessData()

                # fill the Access data field comments
                if projectCode == "VMM" and len(fieldFile) > 0 :
                    FillAccessFieldComments(fieldFile)
                
                if ltGtFound :
                    MoveLtGtRowToTop()

            if projectCode != "VMM" :
                # Field data, get depth and temperature
                GetDepthAndTemp(inputFile)

            # check the data looks valid
            SanityChecks(sampleDate)

            # write the output Access data file
            WriteAccessDataFile(projectDataFiles[projectCode], YearMonthDay(sampleDate))
            
            recordCount = recordCount + len(accessData)

            if fileMove and (warningCount == 0 or interactive):
                MoveCompletedFile(inputFile, "."+os.sep+"For Script", "Raw Data Archive")
                
        CloseWarning()
    else :
        print("No input files found for project "+projectCode)
    
if noFilesFound :
    print("Warning: No input files found to process.")
    warningCount = warningCount + 1

## Get elapsed time of program duration
elapsed_time = (time.time() - start_time) * 1000
print('{}{}{}{}{}{:4.1f} {}'.format("Created ", recordCount, " data entries with ", warningCount, " warnings in ", elapsed_time, "milliseconds."))

exit(0)