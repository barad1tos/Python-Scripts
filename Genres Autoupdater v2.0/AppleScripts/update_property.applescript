(*
    The script to update the track property.
    Use: Osascript Update_Property.Applescript TrackID Propertyname PropertyValue

    Main changes:
    ¥ TrackID is converted to a number for faster comparison.
    ¥ Reference to the Music application is performed in one Tell block.
    ¥ The logic remains unchanged -only the property of Name, Album or Genre is updated.
*)

on run argv
    try
        -- Convert the first argument (TrackID) into a number (if possible)
        set tID to item 1 of argv as integer
        set propName to item 2 of argv
        set propValue to item 3 of argv

        tell application "Music"
            set aTrack to (first track of library playlist 1 whose id is tID)
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

        return "Success: Updated track " & tID & " " & propName & " to " & propValue
    on error errMsg
        return "Error: " & errMsg
    end try
end run