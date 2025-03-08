(*
    The script for getting track parameters from the Music library.
    Logic: If an artist is specified, only his tracks are processed, otherwise all tracks are processed.
    For each track you get: id, name, artist, album, genre, date added (in the format "YYYY-MM-DD HH:MM:SS") and cloud status.
    The result is formed as strings, where the fields are separated by "~|~" characters and the strings are separated by a newline.
*)

on run argv
    -- Getting the artist (if specified)
    set selectedArtist to ""
    if (count of argv) > 0 then
        set selectedArtist to item 1 of argv
    end if

    -- Batch query to Music to get all the necessary properties
    tell application "Music"
        if selectedArtist is not "" then
            set tracksData to get {id, name, artist, album, genre, date added, cloud status} of (every track of library playlist 1 whose artist is selectedArtist)
        else
            set tracksData to get {id, name, artist, album, genre, date added, cloud status} of (every track of library playlist 1)
        end if
    end tell

    -- Parsing the result: tracksData is a list of 7 elements (lists for each property)
    set trackIDs to item 1 of tracksData
    set trackNames to item 2 of tracksData
    set trackArtists to item 3 of tracksData
    set trackAlbums to item 4 of tracksData
    set trackGenres to item 5 of tracksData
    set trackDates to item 6 of tracksData
    set trackCloudStatuses to item 7 of tracksData

    set totalTracks to count of trackIDs
    set output to ""

    repeat with i from 1 to totalTracks
        try
            set tID to trackIDs's item i as string
            set tName to trackNames's item i as string
            set tArtist to trackArtists's item i as string
            set tAlbum to trackAlbums's item i as string
            set tGenre to trackGenres's item i as string
            set tDateRaw to trackDates's item i
            set tCloudRaw to trackCloudStatuses's item i as string
        on error errMsg
            log "Error retrieving properties for track index " & i & ": " & errMsg
            set tID to ""
            set tName to ""
            set tArtist to ""
            set tAlbum to ""
            set tGenre to ""
            set tDateRaw to (current date)
            set tCloudRaw to ""
        end try

        -- Formatting the date (if tDateRaw is not a date - safe conversion)
        set formattedDate to formatDate(tDateRaw)
        
        -- Track status update
        if tCloudRaw is "subscription" then
            set tStatus to "subscription"
        else if tCloudRaw is "prerelease" then
            set tStatus to "prerelease"
        else if tCloudRaw is "downloaded" then
            set tStatus to "downloaded"
        else
            set tStatus to tCloudRaw
        end if

        -- Screen special characters (for example, "|")
        set tID to escape_special_characters(tID)
        set tName to escape_special_characters(tName)
        set tArtist to escape_special_characters(tArtist)
        set tAlbum to escape_special_characters(tAlbum)
        set tGenre to escape_special_characters(tGenre)
        set formattedDate to escape_special_characters(formattedDate)
        set tStatus to escape_special_characters(tStatus)
        
        -- Generate a line for the current track
        set trackLine to tID & "~|~" & tName & "~|~" & tArtist & "~|~" & tAlbum & "~|~" & tGenre & "~|~" & formattedDate & "~|~" & tStatus
        set output to output & trackLine & "\n"
    end repeat
    
    return output
end run

-- The formatDate function: converts the date to the format "YYYY-MM-DD HH:MM:SS"
on formatDate(theDate)
    try
        -- Make sure that theDate is a date; if not, return an empty string
        if class of theDate is date then
            set y to year of theDate
            set mInt to (month of theDate as integer)
            set dInt to day of theDate
            set hhInt to hours of theDate
            set mmInt to minutes of theDate
            set ssInt to seconds of theDate
            
            set mStr to zeroPad(mInt)
            set dStr to zeroPad(dInt)
            set hhStr to zeroPad(hhInt)
            set mmStr to zeroPad(mmInt)
            set ssStr to zeroPad(ssInt)
            
            return (y as string) & "-" & mStr & "-" & dStr & " " & hhStr & ":" & mmStr & ":" & ssStr
        else
            return ""
        end if
    on error errMsg
        log "Error formatting date: " & errMsg
        return ""
    end try
end formatDate

-- zeroPad function: adds a leading zero for numbers less than 10
on zeroPad(numValue)
    try
        if numValue < 10 then
            return "0" & (numValue as string)
        else
            return numValue as string
        end if
    on error errMsg
        log "Error in zeroPad: " & errMsg
        return numValue as string
    end try
end zeroPad

-- The trim function: trims whitespace (space, tab, return, linefeed) from both ends of the string
on trim(str)
    if str is "" then return str
    set whitespaceChars to {" ", tab, return, linefeed}
    repeat while (count of str > 0) and (first character of str is in whitespaceChars)
        set str to text 2 thru -1 of str
    end repeat
    repeat while (count of str > 0) and (last character of str is in whitespaceChars)
        set str to text 1 thru -2 of str
    end repeat
    return str
end trim

-- ReplaceText function: replaces all occurrences of the find character with replace in the subject string
on replaceText(find, replace, subject)
    set AppleScript's text item delimiters to find
    set segments to text items of subject
    set AppleScript's text item delimiters to replace
    set resultStr to segments as string
    set AppleScript's text item delimiters to ""
    return resultStr
end replaceText

-- Function escape_special_characters: escapes the "|" character in a string, except for "subscription" and "prerelease"
on escape_special_characters(input)
    set trimmed_input to trim(input)
    if trimmed_input is "subscription" or trimmed_input is "prerelease" then
        return trimmed_input
    else
        return replaceText("|", "\\|", trimmed_input)
    end if
end escape_special_characters