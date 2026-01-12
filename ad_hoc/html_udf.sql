CREATE OR REPLACE FUNCTION scratch.robinpatel.remove_html(str varchar
                                                         )
    RETURNS varchar
    LANGUAGE JAVASCRIPT
    STRICT
AS
'
var HTMLParsedText=""
var resultSet =  STR.split(''>'')
var resultSetLength =resultSet.length
var counter=0
while(resultSetLength>0)
{
if(resultSet[counter].indexOf(''<'')>0)
{
  var value = resultSet[counter]
  value=value.substring(0, resultSet[counter].indexOf(''<''))
  if (resultSet[counter].indexOf(''&'')>=0 && resultSet[counter].indexOf('';'')>=0)
  {
      value=value.replace(value.substring(resultSet[counter].indexOf(''&''), resultSet[counter].indexOf('';'')+1),'''')
  }
}
  if (value)
  {
    value = value.trim();
    if(HTMLParsedText === "")
    {
        HTMLParsedText = value
    }
    else
    {
      if (value) {
        HTMLParsedText = HTMLParsedText + '' '' + value
      }
    }
    value=''''
  }
  counter= counter+1
resultSetLength=resultSetLength-1
}
HTMLParsedText = HTMLParsedText.replace(/&nbsp;/g, " ");
return HTMLParsedText
    ';



SELECT
    sk.hotel_details,
    se.data.remove_html_from_string(hotel_details)
FROM se.data.sales_kingfisher sk;


SELECT
    sk.hotel_details,
    REGEXP_SUBSTR(sk.hotel_details, '<i>Amenities: (.*)</i>', 1, 1, 'e')
FROM se.data.sales_kingfisher sk;


SELECT
    sk.hotel_details,
    REGEXP_SUBSTR(sk.hotel_details, '<i>Amenities: (.*\\w)</i>', 1, 1, 'e') AS amenities,
    SPLIT(amenities, ', ')
FROM se.data.sales_kingfisher sk;


WITH text AS (
    SELECT
        '<b>Accommodation</b><br><br><b>Hotel Andersen, Copenhagen (two nights)</b><br><br>A seriously cool bolthole in Denmark’s hip capital, Hotel Andersen impresses with chic furnishings, patterned wallpaper, and stylish guest rooms. The aptly-named Cool room is compact yet every bit as fashionable as the rest of the hotel, with hanging lamps, clashing patterns and sumptuous fabrics. Enjoy breakfast each morning at a local café before strolling round cobblestone streets and savouring the slower pace of life of the Scandis.<br><br><i>Amenities: Wi-Fi, terrace, bar, 24-hour reception, room service, fitness centre</i> <br><br><b>Norwegian Getaway cruise ship (nine nights)</b><br><br>There’s plenty to keep you entertained on board; there are 18 decks with more than 28 dining options, five waterslides (one is the fastest on the sea), two swimming pools, four hot tubs, a sports complex and quality nightly entertainment. Add to that a full-service spa with a menu of some 50 treatments, as well as a salt room and a thermal suite, and you''ll be hankering for more than just a week enjoying this impressive vessel.<br><br><i>Amenities: 36 dining and drinking options, theatre and shows, live music, aqua park, casino, spa and wellness facilities, sports complex including rock climbing wall, shopping</i><div><i><br></i></div><div><b>Good to know</b><br><b><br></b></div><div><b>Cancellation and amendments</b>: To find out more about the policy for this offer, please visit the supplier''s <a href="https://www.paramountcruises.com/Terms-And-Conditions">website</a>&nbsp;or call them on 0203 023 7757.<br><br>We cannot guarantee access to hotel facilities (e.g restaurant or spa), as these are subject to change due to COVID-19. We advise you check with the hotel before your trip.     <br><br>To find out more about our ''flexible holidays'', please see our <a href="https://www.secretescapes.com/faq#faq-flexible-holidays">FAQ</a> page.<div><br></div><b>Due to COVID-19, there may be additional entry requirements in effect at your destination, which may change at short notice. Please familiarise yourself with the latest requirements before departure via your government''s foreign advice <a href="https://www.gov.uk/foreign-travel-advice">service</a>.</b></div>' AS text
)
SELECT
    text,
    REGEXP_SUBSTR(text, '<i>Amenities: (.*\\w)</i>', 1, 1, 'e') AS amenities
FROM text




SELECT * FROM latest_vault.sfsc.inclusion i;


