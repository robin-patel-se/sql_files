

 SELECT
	ubr.review_date,
	ubr.customer_score,
	ubr.follow_up_answer,
	IFF(ubr.follow_up_answer IS NOT NULL, snowflake.cortex.TRANSLATE(ubr.follow_up_answer, 'de', 'en'),
		NULL)                                                                                                 AS follow_up_answer_translation
FROM se.data.user_booking_review ubr
LIMIT 10;



 SELECT
	ubr.review_date,
	ubr.customer_score,
	ubr.follow_up_answer,
	IFF(ubr.follow_up_answer IS NOT NULL, snowflake.cortex.TRANSLATE(ubr.follow_up_answer, 'de', 'en'),
		NULL)                                                                                                 AS follow_up_answer_translation
FROM se.data.user_booking_review ubr
LIMIT 10;

/*
 Ich finde es schade, dass in deutschen Hotels alle Anleitungen/Infos in englisch geschrieben werden, das ist zwar für mich kein Problem, aber irgendwie stört es doch.
 */

-- 	I find it a pity that in German hotels all the manuals/info are written in English, although this is not a problem for me, but somehow it still disturbs.





-- interesting!
SELECT snowflake.cortex.translate(NULL, 'de', 'en')

/*
Their work is based on the fact that they are not able to do so. They have been given a lot of time and can be used as
an alternative method for doing things, but they cannot be done without them. Instead, they need to use their own tools
or services. However, they may also be used in other ways. These include: To make sure you don't want to worry about
your business, because they aren't going to get into it, and if you don't know what you're looking for, then you won't
find out how much more. If you don't think about your job, you should take advantage of the way you've gotten there.
You will probably want to go through this process by using our software. But when you see that you're getting stuck
with us, you might just like the ones who are trying to do something else, and if you're thinking about having to do
anything, you could try to keep up with yourself. And if you're talking about these things, you'll feel like you're
really wrong, too, I guess you're not going to be right.". The first thing we did was to give the newly created system,
which has been designed by the company, which is to help us improve the quality of the product, and to create a new
model, which is why we had to offer the same design, which is now available at the marketplace. We were working with
the company, which is currently running the same type of equipment, which is being sold to us. This makes it easy to
build the new one, which is very useful for us. It is possible to buy the new one, which is only available from the
manufacturer, and where we can sell the new one, which is still available from the supplier, which is also available
from the supplier, and which is available from the supplier, which is responsible for the production of the products.
The second thing we found ourselves is to provide the latest generation of the new technology, which is that we have
already made the new version of the new technology, which is available from the manufacturer, which is now available
from the manufacturer, and that we have installed the new technology, which is also available from the supplier, which
is also available from the supplier, which is responsible for the manufacture of the new technologies, such as the new
technology, which is available from
 */

