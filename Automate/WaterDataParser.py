#! python3
"""
This program is used to create a water sample database output file in comma-separated-value 
(csv) format, based on input csv files containing lab or field-measured results. 
The program looks in the "For Script" folder for the required file names. 
Upon completion, the input source files are moved to the "Processed Files" folder, 
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
#   deal with "E.Coli" - done
#   flexicate dupes - done
#   get Alpha analysis names - done
#   add sanity test for duplicate Activity_IDs - done
#   add support for Hydrolab file type - need an example
#   
#   implement CYN reporting values, association - done
#       fix averaging to skip missing values, don't delete rows
#       skip duplicate depth and temp - done
#       average of all is reporting value for all
#       associate all in list
#       calculated, not measured
#       calculate percent - not needed?
#   repair sanity checks for use in Flouro - done
#   support VMM123?
#   clean up, document

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


## @parblock @param [in] fileType String that sets the file suffix to look for.
## @return Returns a list project input files found, with a dictionary of info for each file.@endparblock
## Scans the folder of data files for filename matches, returns a sorted list to be processed.
## Each item in the list is a dictionary of filename, sample date, and if applicable, another
## associated file.
## For VMM lab files, the associated file is a "_forscript_VMMtempdepth.csv" file of 
## the same date.
##    
## Uses global fileSuffixes.
def GetProjectInputFileList(fileType) :
    dir = "For Script"
    SetPath(dir)
    fileList = []
    flgFiles = []
    dateMatch = '2[0-9][0-9][0-9][01][0-9][0-3][0-9]' # this breaks in the year 3000!
    projectInputMatch = "_forscript_"+fileType+".csv"
    flgFiles = fnmatch.filter(os.listdir(dir), dateMatch+projectInputMatch)
    for file in flgFiles :
        auxFile = ""
        fileDate = GetDateTimeObject(file[0:8]).date()
        filepath = dir+os.sep+file

        if fileSuffixes[fileType]["associated"]:
            # see if there is a corresponding Field file, if so, add to list
            auxFile = fnmatch.filter(os.listdir(dir), YearMonthDay(fileDate)+"_forscript_"+fileSuffixes[fileType]["associated"]+".csv")
            if len(auxFile) > 0 :
                auxFile = dir+os.sep+auxFile[0]
        fileList.append({"File":filepath,"Date":fileDate, "Field File":auxFile})
    return(fileList)

## @parblock @param [in] dir Name of folder to seek for input files.@endparblock
## If the folder is not found, it is looked for in the directory above, and if it is found there,
## the script working directory is set to the directory above (..) 
## If the folder for input files "For Script" is not found, the program quits without a warning file.
def SetPath(dir) :
    dirFound = os.path.exists(dir)
    if not dirFound :
        dirFound = os.path.exists(".."+os.sep+dir)
        if dirFound :
            os.chdir("..")
    if not dirFound :
        print("Did not find data file folder '"+dir+"' - Quitting!")
        exit(1)



## @parblock @param [in] fileType Type of file, sets expected column headings for reading the file
## @param [in] labFile String pathname to the lab data file to get the data from.@endparblock
## Reads the lab report file containing the sample data measurements, puts the data into labData.
##
## Uses global fileSuffixes, fills labData.
def GetLabFileData ( fileType, labFile ) :
    
    # By specifying the header names rather than reading them from the file, we avoid bad
    # characters which might occur at the beginning of the file. We also avoid duplicate "Name" columns
    labKeys = fileSuffixes[fileType]["columns"]
        
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
                labData.append(row)
    csvfile.close()
    
    
## @parblock @param [in] testsPerRow List of string test names that appear on the same row of lab data. @endparblock
## Creates a new row of data per measurement, so that the labData has one measure per row.
##
## Rewrites the global labData.
def SerializeData(testsPerRow) :
    global labData
    if testsPerRow :
        passAlongKeys = labData[0].keys() - testsPerRow
        serialLabData = []
        
        for row in labData :
            skipTempDepth = False
            if "analysis_rep" in row.keys() and IsNumber(row["analysis_rep"]) and int(row["analysis_rep"]) > 1 :
                skipTempDepth = True
            for test in testsPerRow :
                if skipTempDepth and test not in fileSuffixes[fileType]["testsToAverage"]:
                    continue
                else :
                    if row[test] : # only fill rows with contents
                        serialRow = {}
                        for key in passAlongKeys :
                            serialRow[key] = row[key]
                        serialRow["Test Name"] = test
                        serialRow["Formatted Entry"] = row[test]
                        serialLabData.append(serialRow)
                
        labData = serialLabData
    
    
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
        
        # This block allows specifying FDUP as FDUP or yes in the FDUP column
        if "FDUP Site" in labRow.keys() and labRow["FDUP Site"] and site != "FDUP" :
            fdup = labRow["FDUP Site"].lower()
            if site in projectSites[projectCode] and (fdup == "fdup" or fdup.find("y") > -1) :
                labData[rowCount]["FDUP Site"] = site
                labData[rowCount]["Site Name"] = "FDUP"
                site = "FDUP"

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
        if labAttributes[lab]["labID"] and not (labRow["Test Name"] == "Depth" or labRow["Test Name"] == "Temperature"):
            accessDataRow["Lab_ID"] = labRow["Text Id"]
        else :
            accessDataRow["Lab_ID"] = "None"
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
        if fileType == "MWRA" and len(labRow["Test Comment"]) > 1 and labRow["Test Comment"] != "nil":
            accessDataRow["Result_Comment"] = labRow["Test Comment"]
        result = labRow["Formatted Entry"]
        accessDataRow["Actual_Result"] = result
        accessDataRow["Actual_Result_Type_ID"] = resultTypes["Actual"]
        if result.find("<") > -1 and IsNumber(result.strip("<")) :
            accessDataRow["Reporting_Result"] = float(result.strip("<"))/2
            accessDataRow["Result_Comment"] = 'Changed censored value, removed "<" symbol, halved value'
            accessDataRow["Reporting_Result_Type_ID"] = resultTypes["Calculated"]
            ltGtFound = True
        elif (result.find(">")) > -1 and IsNumber(result.strip(">")) :
            accessDataRow["Reporting_Result"] = float(result.strip(">"))
            accessDataRow["Result_Comment"] = 'Changed censored value, removed ">" symbol'
            accessDataRow["Reporting_Result_Type_ID"] = resultTypes["Calculated"]
            ltGtFound = True
        elif IsNumber(result) :
            accessDataRow["Reporting_Result"] = result
            accessDataRow["Reporting_Result_Type_ID"] = resultTypes["Actual"]
        else :
            Warning(accessDataRow["Activity_ID"] + " has invalid Formatted Entry result :"+result)
            continue
            
        abbr = GetAnalysisInfo(labRow)["abbrev"]
        if "Display String" in labRow.keys():
            accessDataRow["Actual_Result_Unit_ID"] = unitCodes[labRow["Display String"]]
        else :
            accessDataRow["Actual_Result_Unit_ID"] = analysisNames[lab][abbr]["unitID"]
        accessDataRow["Activity_Type_ID"] = GetActivityType(labRow["Test Name"], site)
        accessDataRow["Result_Sample_Fraction"] = GetAnalysisInfo(labRow)["fraction"]
        accessDataRow["Reporting_Result_Unit_ID"] = accessDataRow["Actual_Result_Unit_ID"]
        accessDataRow["Collection_ID"] = GetCollectionMethod(labRow["Test Name"], site, siteCollectionExceptions)
        accessDataRow["Analytical_Method_ID"] = analysisNames[lab][abbr]["name"]
        accessDataRow["Associated_ID"] = "" # dupe info to be filled in later
        accessDataRow["Data_Type_ID"] = dataTypes["Critical"]
        if labRow["Test Name"] in nonCriticalTests :
            accessDataRow["Data_Type_ID"] = dataTypes["Non-critical"]
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


## @parblock @param [in] testsToAverage List of which tests in a group get averaged together. @endparblock
## There are instances where a single water sample has multiple measurements performed for the same parameter.
## In this case, the multiple values for the parameter are averaged into a reporting value common to each measurement.
## For each repeated measure, 
##     - the measure is reported as the actual value.
##     - the average is reported as the reporting value
##     - the Reporting_Result_Type_ID is reported as "Calculated"
##     - the samples averaged are reported in the Associated_ID
##     - the Result_Comment gets text "Average of n Actual_Result values"
## 
## The routine assumes that rows which have the same activity ID except the last digit get grouped and averaged.
## This routine uses global data siteTestRows, and modifies accessData.
def ApplyAnalysisRepetition(testsToAverage) :
    
    # Create a list from siteTestRows.keys
    # Find each group - look for a row that includes a test to average, then find other rows have the same root activity ID
    #      process the rows in the group
    #      remove the processed rows from the list to search for more groups
    testCodes = []
    for test in testsToAverage :
        testCodes.append(analysisCodes[test]["code"])
    
    toBeProcessed = list(siteTestRows.keys())
    for actID in siteTestRows.keys() :
        if actID in toBeProcessed and accessData[siteTestRows[actID]]["Component_ID"] in testCodes:
            rootID = actID[:-2]
            group = []
            for ID in toBeProcessed:
                if ID.find(rootID) == 0 :
                    group.append(ID)
            if len(group) > 1 :
                average = 0.0
                for ID in group :
                    average += float(accessData[siteTestRows[ID]]["Actual_Result"])
                average = '{:4.2f}'.format(average/len(group), 0)
                for ID in group :
                    accessData[siteTestRows[ID]]["Reporting_Result"] = average
                    accessData[siteTestRows[ID]]["Reporting_Result_Type_ID"] = resultTypes["Calculated"]
                    subgroup = group.copy()
                    subgroup.remove(ID)
                    accessData[siteTestRows[ID]]["Associated_ID"] = ", ".join(subgroup)
                    accessData[siteTestRows[ID]]["Result_Comment"] = "Average of "+str(len(group)) + " Actual_Result values"
                    toBeProcessed.remove(ID)
                    
        else :
            if actID in toBeProcessed :
                toBeProcessed.remove(actID)
    

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
    site = site_info[-1]
#    if "FDUP Site" in rowData.keys() and rowData["FDUP Site"] and site != "FDUP" :
#        fdup = rowData["FDUP Site"].lower()
#        if site in projectSites[projectCode] and (fdup == "fdup" or fdup.find("y") > -1) :
#            rowData["FDUP Site"] = site
#            site = "FDUP"
#            print(site, rowData["FDUP Site"])
    return(site)

## @parblock @param [in] rowData Dictionary of one row of sample lab data
## @return datetime object constructed from the sample date and time.@endparblock
##  Lab data for the sample time is of the form mo/day/yr hr:min:00
##  This routine returns a datetime object constructed based on that format.
def GetSampleDateTime(rowData):
    if "Sampled Time" in rowData.keys():
        return(GetDateTimeObject(rowData["Sampled Date"]+" "+rowData["Sampled Time"]))
    return(GetDateTimeObject(rowData["Sampled Date"]))

## @parblock @param [in] rowData Dictionary of one row of sample lab data
## @return Dictionary of analysisCodes info for the specified test type.@endparblock
## Returns the analysisCode dictionary of analysis test information based on the rowData Test Name.
def GetAnalysisInfo(rowData):
    parameter = rowData["Test Name"]
    if parameter not in analysisCodes.keys():
        parameter = WarningWithReplace("Found unknown parameter: '"+str(parameter)+"' Legal values are " +", ".join(analysisCodes.keys()))
        rowData["Test Name"] = parameter
    return(analysisCodes[parameter])
    
## @parblock @param [in] projectCode Project code string, such as FLG, VMM, etc.
## @param [in] rowData Dictionary of one row of sample lab data
## @return String Activity Identifier@endparblock
##  The Activity ID field is a concatenation of the project abbreviation, the date, the site, 
##  the test performed, and the sample count. 
def GetActivityId(projectCode, rowData):
    sampleDate = GetSampleDateTime(rowData).date()
    site = GetSiteId(rowData)
    abbr = GetAnalysisInfo(rowData)["abbrev"]
    if "analysis_rep" in rowData.keys() and IsNumber(rowData["analysis_rep"]):
        count = "0"+rowData["analysis_rep"]
    else :
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
    if projectCode == "CYN" :
        return(activityCodes["Field Msr/Obs-Portable Data Logger"])
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
    if projectCode == "CYN" :
        method = "C-MGBN"
    elif projectCode == "FLG" :
        method = "C-MGBO"
    elif site in siteCollections.keys():
        method = siteCollections[site]
    else:
        method = "C-BABR"
    if measure == "Temperature":
        method = method.replace("C-", "N-")
    return(method)



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
##        - 2020/01/21 6:00 AM
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
                                try :
                                    dt = datetime.strptime(timeStr, "%Y/%m/%d %I:%M %p") # 2020/01/21 6:00 AM
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
        #legalMethods.append(analysisNames[lab][analysisCodes[key]["abbrev"]])
        legalFractions.append(analysisCodes[key]["fraction"])
        legalLimits[analysisCodes[key]["code"]] = {"test":key, "lower":analysisCodes[key]["lower"], "upper":analysisCodes[key]["upper"]}
        
    for key in analysisNames[lab] :
        legalMethods.append(analysisNames[lab][key]["name"])
    
    legalUnits = []
    for unit in unitCodes.keys() :
        legalUnits.append(unitCodes[unit])
    legalActivities = []
    for key in activityCodes :
        legalActivities.append(activityCodes[key])
    
    legalCollects = ["C-BABR","C-SPBR", "C-MGW", "C-SPBN", "C-BABN", "C-MGBO", "N-DL","N-BABR","N-SPBR", "N-MGW", "N-SPBN", "N-BABN", "N-MGBO", "C-MGBN", "N-MGBN"]
    
    prj = projectCode
    if projectCode == "Field":
        prj = "VMM"
    activityIds = []
    idCheck = {}
    for row in accessData :
        activityIds.append(row["Activity_ID"])
        idCheck[row["Activity_ID"]] = 1
        
    if len(activityIds) != len(idCheck.keys()) :
        ActIdQ = activityIds.copy()
        for actId in idCheck.keys() :
            ActIdQ.remove(actId)
        Warning("Duplicate Activity_ID values: "+", ".join(ActIdQ))
    
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
        assoc_id = row[field]
        if len(assoc_id) :
            # should have dupe name
            idList = assoc_id.split(sep = ", ")
            for id in idList :
                #id.strip()
                if id not in activityIds :
                    Warning("Site "+site+" "+field + " field error: "+id+" not found in Activity_IDs")
                if (row["Activity_ID"][:-2] != id[:-2]) :
                    Warning("Site "+site+" "+field + " field error: "+id +" does have the same prefix as the Activity_ID "+row["Activity_ID"])
        field = "Media_Subdivision_ID"
        id = row[field]
        if id != 21 :
            Warning("Site "+site+" "+field + " field error: "+id)
        field = "Result_Comment"
        id = row[field]
        if len(id) > 0 and (id.find("Changed censored value,") < 0 or (row["Actual_Result"][0] != "<" and row["Actual_Result"][0] != ">")) and (id.find("Average of ") < 0) :
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
        filename = "."+os.sep+"For Script"+os.sep+"Warnings_"+YearMonthDay(sampleDate)+"_"+fileType+".txt"
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

## Dictionary containing the partial file names to use in naming input files, and info associated with each file type.
fileSuffixes = {"MWRA":{"project":"VMM", "lab":"MWRA", "testsPerRow":[], "associated":"VMMtempdepth", 
                        "columns":("Sample Number","Text Id","Site Name","Description","X Trip","Sampled By","Test Location","Status","Sampled Date","Analyzed On","Analysis","Test Name","Formatted Entry","Display String","Batch","X Result Flags","FDUP Site","X Sample Flags","Test Comment")}, 
                "VMMtempdepth":{"project":"VMM", "lab":"Field", "testsPerRow":["Temperature", "Depth"], "associated":"", 
                                "columns":("Site Name", "Sampled Date","Temperature", "Depth", "Comment")},
                #"VMM123tempdepth":{"project":"VMM", "lab":"Field", "testsPerRow":["Temperature", "Depth"], "associated":"", 
                #                "columns":("Site Name", "x", "x", "x", "x","Temperature", "x", "x", "Depth", "x", "x", "x", "Comment","Sampled Date",)},
                "Flagging":{"project":"FLG", "lab":"G&L", "testsPerRow":["E. coli","Temperature", "Depth"], "associated":"", 
                            "columns":("Text Id", "Sampled Date", "Site Name", "E. coli", "Temperature", "Depth", "Comment", "FDUP Site")},
                "AlphaLabResults":{"project":"VMM", "lab":"Alpha", "testsPerRow":[], "associated":"VMMtempdepth", 
                             "columns":("Text Id", "Site Name", "Sampled Date", "Test Name", "Formatted Entry", "FDUP Site")},
                "Fluoro":{"project":"CYN", "lab":"Fluorometer", "testsPerRow":["Temperature", "Chlorophyll A", "Cyanophyta density", "Depth"], "associated":"", 
                          "columns":("x","x","x","x","x","x","x","x","Site Name","x","x","x","x","x","Text Id","Sampled Date", "Sampled Time", "x","x", "Temperature", "x","x","x","x", "Chlorophyll A", "Cyanophyta density", "analysis_rep", "x","Comment", "x", "Depth"),
                         "testsToAverage":["Chlorophyll A", "Cyanophyta density"]},
                "Hydrolab":{"project":"CYN", "lab":"Hydrolab", "testsPerRow":[], "associated":"", "columns":()}
               }


## Dictionary of activity codes keyed by name
activityCodes = {"Sample-Routine":6, "Quality Control Sample-Field Replicate":5, "Field Msr/Obs":1, "Field Msr/Obs-Portable Data Logger":2}

## Dictionary of result type codes keyed by name
resultTypes = {"Actual":1, "Calculated":2}

## Dictionary of unit codes keyed by name
unitCodes = {"MPN/100ml":10, "MPN/100 mL":10, "ug/L":13, "mg/L":7, "deg C":4, "ft":5, "cfu/100ml":3, "cells/ml":2, "volts":16, "% Sat":1, "cells/ml":2, "m":6, "pH units":11, "ppth":12, "uS/cm":15}

## Dictionary of analyses indexed by name, with abbreviation, component code, sample fraction, lower and upper limit
analysisCodes ={"E. coli":{"abbrev":"EC", "code":12, "fraction":"Total", "lower":0.0, "upper":30000.0},
               "Chlorophyll A":{"abbrev":"CA", "code":6, "fraction":"Total", "lower":0.0, "upper":200.0},
               "Phaeophytin":{"abbrev":"PP", "code":20, "fraction":"Total", "lower":0.0, "upper":20.0},
               "PO4-P":{"abbrev":"OP", "code":18, "fraction":"Dissolved", "lower":0.0, "upper":0.08},
               "NO32-N":{"abbrev":"NN", "code":14, "fraction":"Total", "lower":0.0, "upper":10.0},
               "NH3-N":{"abbrev":"NH3", "code":2, "fraction":"Dissolved", "lower":0.0, "upper":0.6},
               "Enterococci":{"abbrev":"ENT", "code":11, "fraction":"Total", "lower":0.0, "upper":20000.0},
               "TN":{"abbrev":"TN", "code":17, "fraction":"Total", "lower":0.0, "upper":15.0},
               "TP":{"abbrev":"TP", "code":21, "fraction":"Total", "lower":0.0, "upper":8.0},
               "TSS":{"abbrev":"TSS", "code":27, "fraction":"Total", "lower":0.0, "upper":100.0},
               "Depth":{"abbrev":"DTH", "code":8, "fraction":"N/A", "lower":0.25, "upper":35.0},
               "Temperature":{"abbrev":"Temp", "code":26, "fraction":"N/A", "lower":-1.0, "upper":45.0},
               "Dissolved Oxygen Saturation":{"abbrev":"DO%", "code":10, "fraction":"N/A", "lower":0.0, "upper":200.0},
               "Dissolved Oxygen":{"abbrev":"DO", "code":9, "fraction":"N/A", "lower":0.0, "upper":25.0},
               "Fecal coliform":{"abbrev":"FC", "code":13, "fraction":"Total", "lower":0.0, "upper":450000.0},
               "Sodium":{"abbrev":"NA", "code":23, "fraction":"Total", "lower":0.0, "upper":300.0},
               "Surfactants":{"abbrev":"SFT", "code":25, "fraction":"Total", "lower":0.0, "upper":1.0},
               "Cyanophyta voltage":{"abbrev":"PCYV", "code":1, "fraction":"N/A", "lower":0.0, "upper":250000.0},
               "Cyanophyta density":{"abbrev":"PCY", "code":1, "fraction":"N/A", "lower":0.0, "upper":40000.0},
               "pH":{"abbrev":"PH", "code":19, "fraction":"N/A", "lower":6.0, "upper":9.0},
               "Salinity":{"abbrev":"SAL", "code":22, "fraction":"N/A", "lower":-0.05, "upper":7.0},
               "Specific conductance":{"abbrev":"SC", "code":24, "fraction":"N/A", "lower":0.0, "upper":60000.0}
               }
analysisCodes["E. Coli"] = analysisCodes["E. coli"] # spelling tolerance

## Dictionary per lab for the analysis names and units to apply to a given test type. The analyses are keyed by the analysis
## abbreviation. For example, analysisNames["G&L"]["EC"]["name"] gives "G&L-EC-2012".
## The units are specified because the units can differ by lab.
analysisNames = {"MWRA":{"EC":{"name":"MWRA-EC-2012", "unitID":10}, "CA":{"name":"MWRA-ChlorA-2012", "unitID":13},"PP":{"name":"MWRA-Phaeo-2012", "unitID":13},"OP":{"name":"MWRA-OPD-2012", "unitID":7},"NN":{"name":"MWRA-N/N-2012", "unitID":7}, "NH3":{"name":"MWRA-NH3D-2012", "unitID":7}, "ENT":{"name":"MWRA-Ent-2012", "unitID":10}, "TN":{"name":"MWRA-TN-2012", "unitID":7}, "TP":{"name":"MWRA-TP-2012", "unitID":7}, "TSS":{"name":"MWRA-TSS-2012", "unitID":7}},

"Alpha":{"EC":{"name":"Alpha-EC-2012", "unitID":10}, "CA":{"name":"Alpha-ChlorA-2012", "unitID":13},"OP":{"name":"Alpha-OPD-2012", "unitID":7},"NN":{"name":"Alpha-N/N-2012", "unitID":7}, "NH3":{"name":"Alpha-NH3D-2012", "unitID":7}, "ENT":{"name":"Alpha-Ent-2012", "unitID":10}, "TN":{"name":"Alpha-TN-2012", "unitID":7}, "TP":{"name":"Alpha-TP-2012", "unitID":7}, "TSS":{"name":"Alpha-TSS-2012", "unitID":7},"FC":{"name":"Alpha-FC-2012", "unitID":10},"NA":{"name":"Alpha-NA-2012", "unitID":7},"SFT":{"name":"Alpha-SFT-2012", "unitID":12}},

"Field":{"DTH":{"name":"Field-Depth-2012", "unitID":5}, "Temp":{"name":"Therm-Temp-2012", "unitID":4}},

"G&L":{"EC":{"name":"G&L-EC-2012", "unitID":10}},
        
"Hydrolab":{"DO%":{"name":"Hydrolab-DO-2012", "unitID":1}, "CA":{"name":"ChlorA-Beagle", "unitID":13}, "DO":{"name":"Hydrolab-DO-2012", "unitID":7},"PCYV":{"name":"Hydrolab-PCYV-2012", "unitID":2}, "PCY":{"name":"Hydrolab-PCY-2012", "unitID":16}, "PH":{"name":"Hydrolab-pH-2012", "unitID":11}, "SAL":{"name":"Hydrolab-Salinity-2012", "unitID":12}, "SC":{"name":"Hydrolab-SC-2012", "unitID":15}},

"Fluorometer":{"CA":{"name":"ChlorA-Beagle", "unitID":13},"PCY":{"name":"PCY-Beagle", "unitID":13}}
}
# copy temperature and depth info to other lab types
for fieldParameter in analysisNames["Field"].keys():
    analysisNames["Fluorometer"][fieldParameter] = analysisNames["Field"][fieldParameter]
    analysisNames["Hydrolab"][fieldParameter] = analysisNames["Field"][fieldParameter]
    analysisNames["G&L"][fieldParameter] = analysisNames["Field"][fieldParameter]
    

## Dictionary per fileType, indicating whether the labID is used, and whether dupes are supported.
labAttributes = {"MWRA":{"labID":True, "dupeSupport":True},
                "Field":{"labID":False, "dupeSupport":False},
                "Alpha":{"labID":True, "dupeSupport":True},
                "G&L":{"labID":True, "dupeSupport":True},
                "Hydrolab":{"labID":False, "dupeSupport":True},
                "Fluorometer":{"labID":False, "dupeSupport":False}
                }

## List of tests that are always non-critical.
nonCriticalTests = ("Depth", "Temperature")
    
## Maximum time difference, in minutes, allowed between reported Time_Collected values for a given site and date.
## Times that are larger produce a warning.
maxTimeDiff = 30.0

## Maximum time difference, in days, allowed between reported Date_Collected values and the date in the input file name.
## Date differences that are larger produce a warning.
maxDateDiff = 42.0

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
                    27:{"diff":0.0,"percent":100}, # TSS
                     1:{"diff":0.0,"percent":100}, # cyanobacteria
                    19:{"diff":0.0,"percent":100}, # pH
                    22:{"diff":0.0,"percent":100}, # salinity
                    24:{"diff":0.0,"percent":100}, # SC
                     9:{"diff":0.0,"percent":100}, # DO
                    10:{"diff":0.0,"percent":100}  # DO%
                   } 

## Dictionary giving tuples of site names legal per project, keyed by project name
projectSites = {"VMM":("35CS","59CS","90CS","130S","165S","199S","229S","267S","269T","290S","318S","343S","387S","400S","447S","484S","521S","534S","567S","591S","609S","621S","635S","648S","662S","675S","012S","700S","715S","729S","743S","760T","763S","773S","784S", "MBD", "MBU", "HBD", "HBU", "CBU", "CBD", "MB-D", "MB-U", "HB-D", "HB-U", "CB-U", "CB-D", "QC","ROV1","ROV2"),
"FLG":("1NBS","2LARZ","3BU","4LONG"), "CYN": ("ROB", "BROAD", "SP", "CB", "ND", "621S", "BR", "MOS", "FG1", "FG2", "FG3")}
projectSites["Field"] = projectSites["VMM"]

## Site collection type at each site uses "C-BABR" except these exception sites
siteCollectionExceptions = {"35CS":"C-SPBR", "199S":"C-MGW", "267S":"C-SPBN", "447S":"C-SPBN", "635S":"C-BABN", "648S":"C-MGBO", "CBU":"C-SPBN"}

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

for fileType in fileSuffixes.keys():
    
    ## Project code from the fileType.
    projectCode = fileSuffixes[fileType]["project"]
    
    ## Which lab performs the analysis
    lab = fileSuffixes[fileType]["lab"]
    
    ## List of files to process for this project, each file is in a tuple of info.
    fileList = GetProjectInputFileList(fileType)

    if len(fileList) > 0 :
        noFilesFound = False
        for processFileInfo in fileList:
            ## File name to process for data
            inputFile = processFileInfo["File"]
            
            ## Sample datetime date object from input filename
            sampleDate = processFileInfo["Date"]
            
            ## Auxilliary file used for VMM site comments, empty except for VMM
            fieldFile = processFileInfo["Field File"]
            
            if fileSuffixes[fileType]["associated"] and not fieldFile :
                Warning(fileSuffixes[fileType]["associated"]+" file not found to go with "+inputFile+"; no field comments available.")
            
            ## This list of dictionaries contains the data to output. The output data is populated from the 
            ## input data per rules coded in FillAccessData(), FillAccessFieldComments(), and FillDupeAccessData().
            accessData = []

            ## Keep track of sites processed
            siteRows = []
        
            ## This list of dictionaries contains the data from the input file.
            labData = []
                         
            GetLabFileData(fileType, inputFile)

            SerializeData(fileSuffixes[fileType]["testsPerRow"])

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
            if labAttributes[lab]["dupeSupport"] :
                FillDupeAccessData()
            
            if "testsToAverage" in fileSuffixes[fileType].keys() :
                ApplyAnalysisRepetition(fileSuffixes[fileType]["testsToAverage"])

            # fill the Access data field comments, when they come from a separate file
            if fileSuffixes[fileType]["associated"] and fieldFile :
                FillAccessFieldComments(fieldFile)

            if ltGtFound :
                MoveLtGtRowToTop()
  
            # check the data looks valid
            SanityChecks(sampleDate)

            # write the output Access data file
            WriteAccessDataFile(fileType, YearMonthDay(sampleDate))
            
            recordCount = recordCount + len(accessData)

            if fileMove and (warningCount == 0 or interactive):
                MoveCompletedFile(inputFile, "."+os.sep+"For Script", "Processed Files")
                
        CloseWarning()
    else :
        print("No input files found for file type "+fileType)
    
if noFilesFound :
    print("Warning: No input files found to process.")
    warningCount = warningCount + 1

## Get elapsed time of program duration
elapsed_time = (time.time() - start_time) * 1000
print('{}{}{}{}{}{:4.1f} {}'.format("Created ", recordCount, " data entries with ", warningCount, " warnings in ", elapsed_time, "milliseconds."))

exit(0)
