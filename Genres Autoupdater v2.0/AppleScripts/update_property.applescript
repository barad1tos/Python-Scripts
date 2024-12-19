-- update_property.applescript
-- This AppleScript updates a specified property (name, album, genre) of a track by its ID.
-- Usage: osascript update_property.applescript trackID propertyName propertyValue

on run argv
    try
        set trackID to item 1 of argv
        set propName to item 2 of argv
        set propValue to item 3 of argv
        tell application "Music"
            set aTrack to (first track of library playlist 1 whose id is trackID)
            if propName is "name" then
                set name of aTrack to propValue
            else if propName is "album" then
                set album of aTrack to propValue
            else if propName is "genre" then
                set genre of aTrack to propValue
            else
                return "Error: Unknown property name " & propName
            end if
        end tell
        return "Success: Updated track " & trackID & " " & propName & " to " & propValue
    on error errMsg
        return "Error: " & errMsg
    end try
end run