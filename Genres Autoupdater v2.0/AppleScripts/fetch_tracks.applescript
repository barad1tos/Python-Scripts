-- Function to remove spaces at the beginning and end of a string
on trim(str)
    -- Removes spaces at the beginning of a line
    repeat while str begins with " " or str begins with tab or str begins with return or str begins with linefeed
        set str to text 2 thru -1 of str
        if str is "" then exit repeat
    end repeat
    
    -- Removes spaces at the end of a line
    repeat while str ends with " " or str ends with tab or str ends with return or str ends with linefeed
        set str to text 1 thru -2 of str
        if str is "" then exit repeat
    end repeat
    
    return str
end trim

-- Function to screen out special characters
on escape_special_characters(input)
    set trimmed_input to my trim(input)
    if trimmed_input is "subscription" then
        log "Escape: input is 'subscription', returning as is."
        return trimmed_input
    else if trimmed_input is "prerelease" then
        log "Escape: input is 'prerelease', returning as is."
        return trimmed_input
    else
        set input to my replaceText("|", "\\|", trimmed_input)
        log "Escaped input: " & input
        return input
    end if
end escape_special_characters

-- Function to replace text
on replaceText(find, replace, subject)
    set AppleScript's text item delimiters to find
    set segments to text items of subject
    set AppleScript's text item delimiters to replace
    set subject to segments as string
    set AppleScript's text item delimiters to ""
    return subject
end replaceText

-- Function for getting track status
on get_track_status(aTrack)
    try
        tell application "Music"
            -- Set cloudStatus to the string
            set cloudStatus to (cloud status of aTrack) as string
            set cloudStatus to my trim(cloudStatus)
            log "Track ID: " & id of aTrack & ", Cloud Status: " & cloudStatus
            
            -- Status determination based on cloud status
            if cloudStatus is "subscription" then
                set statusResult to "subscription"
            else if cloudStatus is "prerelease" then
                set statusResult to "prerelease"
            else if cloudStatus is "downloaded" then
                set statusResult to "downloaded"
            else if cloudStatus is "available" then
                set statusResult to "available"
            else if cloudStatus is "not-supported" then
                set statusResult to "not-supported"
            else
                set statusResult to "unknown"
            end if
        end tell
        return statusResult
    on error errMsg
        log "Error getting status for track ID " & (id of aTrack as string) & ": " & errMsg
        return "unknown"
    end try
end get_track_status

-- Main function of the script
on run argv
    set theTracks to {}
    set selectedArtist to ""
    
    if (count of argv) > 0 then
        set selectedArtist to item 1 of argv
    end if
    
    tell application "Music"
        if selectedArtist is not "" then
            try
                set tracksByArtist to (every track of playlist 1 whose artist is selectedArtist)
            on error
                log "Error: Playlist was not found or does not contain tracks from artist = " & selectedArtist
                return "unknown"
            end try
            set totalTracks to count of tracksByArtist
            set processedTracks to 0
            
            log "Fetching tracks for artist: " & selectedArtist
            
            repeat with t in tracksByArtist
                set processedTracks to processedTracks + 1
                try
                    set trackID to my escape_special_characters(id of t as string)
                    set trackName to my escape_special_characters(name of t as string)
                    set trackArtist to my escape_special_characters(artist of t as string)
                    set trackAlbum to my escape_special_characters(album of t as string)
                    set trackGenre to my escape_special_characters(genre of t as string)
                    set trackDate to my escape_special_characters(date added of t as string)
                    
                    set rawStatus to my get_track_status(t)
                    log "Raw Track Status: " & rawStatus
 
                    set escapedStatus to rawStatus
                    log "Escaped Track Status: " & escapedStatus
                    set trackStatus to escapedStatus
                    
                    -- Additional logging of each field
                    log "Track ID: " & trackID
                    log "Track Name: " & trackName
                    log "Track Artist: " & trackArtist
                    log "Track Album: " & trackAlbum
                    log "Track Genre: " & trackGenre
                    log "Track Date: " & trackDate
                    log "Track Status: " & trackStatus
                    
                    set trackLine to trackID & "~|~" & trackName & "~|~" & trackArtist & "~|~" & trackAlbum & "~|~" & trackGenre & "~|~" & trackDate & "~|~" & trackStatus
                    log "Track Line: " & trackLine
                    set end of theTracks to trackLine
                    log "Processed " & processedTracks & " of " & totalTracks & ": " & trackName & " by " & trackArtist
                on error errMsg
                    log ("Error processing track: " & errMsg)
                end try
            end repeat
        else
            try
                set allTracks to (every track of playlist 1)
            on error
                log "Error: Playlist was not found or does not contain tracks."
                return "unknown"
            end try
            set totalTracks to count of allTracks
            set processedTracks to 0
            
            log "Fetching all tracks from playlist 1"
            
            repeat with t in allTracks
                set processedTracks to processedTracks + 1
                try
                    set trackID to my escape_special_characters(id of t as string)
                    set trackName to my escape_special_characters(name of t as string)
                    set trackArtist to my escape_special_characters(artist of t as string)
                    set trackAlbum to my escape_special_characters(album of t as string)
                    set trackGenre to my escape_special_characters(genre of t as string)
                    set trackDate to my escape_special_characters(date added of t as string)
                    
                    set rawStatus to my get_track_status(t)
                    log "Raw Track Status: " & rawStatus
 
                    set escapedStatus to rawStatus
                    log "Escaped Track Status: " & escapedStatus
                    set trackStatus to escapedStatus
                    
                    -- Additional logging of each field
                    log "Track ID: " & trackID
                    log "Track Name: " & trackName
                    log "Track Artist: " & trackArtist
                    log "Track Album: " & trackAlbum
                    log "Track Genre: " & trackGenre
                    log "Track Date: " & trackDate
                    log "Track Status: " & trackStatus
                    
                    set trackLine to trackID & "~|~" & trackName & "~|~" & trackArtist & "~|~" & trackAlbum & "~|~" & trackGenre & "~|~" & trackDate & "~|~" & trackStatus
                    log "Track Line: " & trackLine
                    set end of theTracks to trackLine
                    log "Processed " & processedTracks & " of " & totalTracks & ": " & trackName & " by " & trackArtist
                on error errMsg
                    log ("Error processing track: " & errMsg)
                end try
            end repeat
        end if
    end tell
    
    set output to ""
    repeat with track in theTracks
        set output to output & track & "\n"
    end repeat
    
    -- Writing to a file for testing
--    set test_output_file to "/Users/romanborodavkin/Music/outputs.txt"
--    try
--        set file_ref to open for access POSIX file test_output_file with write permission
--        set eof of file_ref to 0
--        repeat with track in theTracks
--            write track & "\n" to file_ref
--        end repeat
--        close access file_ref
--        log "Test output written to " & test_output_file
--    on error errMsg
--        try
--            close access POSIX file test_output_file
--        end try
--        log "Error writing test output: " & errMsg
--    end try
--    
--    log "Script completed successfully."
    
    return output
end run