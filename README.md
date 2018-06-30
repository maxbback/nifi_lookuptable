# nifi_lookuptable
Use NiFi Groovy processor with a lookup table to anonymize or enrich your data

This is a NiFi processor written in Groovy and is intended to anonymyse data when we have a match in a lookup table.

My example code is using an address table of Estonian addresses as they was published for free download and I use it to anonymize my text in my case I have the address list in a text file and is anonymizing this.
So all rows in my text should match and be anonymized.

There is a full article describing this process in more detail, you find it here
http://max.bback.se/index.php/2018/06/07/lookup-table-to-mask-or-extend-a-feed-in-nifi-with-groovy/
