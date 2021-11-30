WITH canvassing_cte AS (
	SELECT c.vanid
		, RANK () OVER (PARTITION BY c.vanid ORDER BY c.datecanvassed ASC)
		, c.contactscontactid
		, c.datecanvassed
	    , r.resultshortname AS results
	    , u.firstname||' '||u.lastname AS canvasser
	    , cam.campaignname AS campaign
	    , c.vanid||c.canvassedby||LEFT(c.datecanvassed,10) AS concat_field
	FROM tmc_van.aya_contactscontacts_mym c
	LEFT JOIN tmc_van.tsm_tmc_results r
		USING (resultid)
	LEFT JOIN tmc_van.tsm_tmc_users u
		ON u.userid = c.canvassedby
	LEFT JOIN tmc_van.aya_campaigns cam
		USING (campaignid)
	WHERE cam.campaignname = 'CAMPAIGN NAME HERE' 
	ORDER BY 1,2 DESC
)

SELECT m.vanid
	, CASE WHEN n.notecategoryname = 'Rental Court Case #' THEN n.notecategoryname ELSE NULL END AS rental_court_case_num
	, m.firstname
	, m.lastname
	, a.addressline1
	, a.addressline2
	, a.city
	, a.state
	, a.zip5
	, p.phone
	, pt.phonetypename AS phone_type
	, e.email
--First canvass attempt data
    , CASE WHEN c.rank = 1 THEN c.datecanvassed ELSE NULL END AS c1_date
    , CASE WHEN c.rank = 1 THEN c.results ELSE NULL END AS c1_results
    , CASE WHEN c.rank = 1  c.canvasser ELSE NULL END AS c1_canvasser
    , CASE WHEN (c.rank = 1 AND n.notecategoryname = 'Cera # and Status') THEN n.notetext ELSE NULL END AS ceranum_status
    , CASE WHEN (c.rank = 1 AND n.notecategoryname = 'UCHC Case Type') THEN n.notetext ELSE NULL END AS uchc_casetype
    , CASE WHEN (c.rank = 1 AND n.notecategoryname = 'General UCHC Canvassing Notes (stand out info, details, clarifications, data edits, etc)') THEN n.notetext ELSE NULL END AS canvass_notes
    , CASE WHEN (c.rank = 1 AND n.notecategoryname = 'Rental, Immediate Eviction Notes') THEN n.notetext ELSE NULL END AS rental_eviction_notes
    , CASE WHEN (c.rank = 1 AND n.notecategoryname = 'Rental Campaign, Vulnurable Population Notes') THEN n.notetext ELSE NULL END AS rentcam_vulpop_notes
--Second canvass attempt data
    , CASE WHEN c.rank = 2 THEN c.datecanvassed ELSE NULL END AS c2_date
    , CASE WHEN c.rank = 2 THEN c.results ELSE NULL END AS c2_results
    , CASE WHEN c.rank = 2 THEN c.canvasser ELSE NULL END AS c2_canvasser
    , CASE WHEN (c.rank = 2 AND n.notecategoryname = 'Cera # and Status') THEN n.notetext ELSE NULL END AS ceranum_status
    , CASE WHEN (c.rank = 2 AND n.notecategoryname = 'UCHC Case Type') THEN n.notetext ELSE NULL END AS uchc_casetype
    , CASE WHEN (c.rank = 2 AND n.notecategoryname = 'General UCHC Canvassing Notes (stand out info, details, clarifications, data edits, etc)') THEN n.notetext ELSE NULL END AS canvass_notes
    , CASE WHEN (c.rank = 2 AND n.notecategoryname = 'Rental, Immediate Eviction Notes') THEN n.notetext ELSE NULL END AS rental_eviction_notes
    , CASE WHEN (c.rank = 2 AND n.notecategoryname = 'Rental Campaign, Vulnurable Population Notes') THEN n.notetext ELSE NULL END AS rentcam_vulpop_notes
--Third canvass attempt data
    , CASE WHEN c.rank = 3 THEN c.datecanvassed ELSE NULL END AS c3_date
    , CASE WHEN c.rank = 3 THEN c.results ELSE NULL END AS c3_results
    , CASE WHEN c.rank = 3 THEN c.canvasser ELSE NULL END AS c3_canvasser
    , CASE WHEN (c.rank = 3 AND n.notecategoryname = 'Cera # and Status') THEN n.notetext ELSE NULL END AS ceranum_status
    , CASE WHEN (c.rank = 3 AND n.notecategoryname = 'UCHC Case Type') THEN n.notetext ELSE NULL END AS uchc_casetype
    , CASE WHEN (c.rank = 3 AND n.notecategoryname = 'General UCHC Canvassing Notes (stand out info, details, clarifications, data edits, etc)') THEN n.notetext ELSE NULL END AS canvass_notes
    , CASE WHEN (c.rank = 3 AND n.notecategoryname = 'Rental, Immediate Eviction Notes') THEN n.notetext ELSE NULL END AS rental_eviction_notes
    , CASE WHEN (c.rank = 3 AND n.notecategoryname = 'Rental Campaign, Vulnurable Population Notes') THEN n.notetext ELSE NULL END AS rentcam_vulpop_notes
--Fourth canvass attempt data
    , CASE WHEN c.rank = 4 THEN c.datecanvassed ELSE NULL END AS c4_date
    , CASE WHEN c.rank = 4 THEN c.results ELSE NULL END AS c4_results
    , CASE WHEN c.rank = 4 THEN c.canvasser ELSE NULL END AS c4_canvasser
    , CASE WHEN (c.rank = 4 AND n.notecategoryname = 'Cera # and Status') THEN n.notetext ELSE NULL END AS ceranum_status
    , CASE WHEN (c.rank = 4 AND n.notecategoryname = 'UCHC Case Type') THEN n.notetext ELSE NULL END AS uchc_casetype
    , CASE WHEN (c.rank = 4 AND n.notecategoryname = 'General UCHC Canvassing Notes (stand out info, details, clarifications, data edits, etc)') THEN n.notetext ELSE NULL END AS canvass_notes
    , CASE WHEN (c.rank = 4 AND n.notecategoryname = 'Rental, Immediate Eviction Notes') THEN n.notetext ELSE NULL END AS rental_eviction_notes
    , CASE WHEN (c.rank = 4 AND n.notecategoryname = 'Rental Campaign, Vulnurable Population Notes') THEN n.notetext ELSE NULL END AS rentcam_vulpop_notes
--Fifth canvass attempt data
    , CASE WHEN c.rank = 5 THEN c.datecanvassed ELSE NULL END AS c5_date
    , CASE WHEN c.rank = 5 THEN c.results ELSE NULL END AS c5_results
    , CASE WHEN c.rank = 5 THEN c.canvasser ELSE NULL END AS c5_canvasser
    , CASE WHEN (c.rank = 5 AND n.notecategoryname = 'Cera # and Status') THEN n.notetext ELSE NULL END AS ceranum_status
    , CASE WHEN (c.rank = 5 AND n.notecategoryname = 'UCHC Case Type') THEN n.notetext ELSE NULL END AS uchc_casetype
    , CASE WHEN (c.rank = 5 AND n.notecategoryname = 'General UCHC Canvassing Notes (stand out info, details, clarifications, data edits, etc)') THEN n.notetext ELSE NULL END AS canvass_notes
    , CASE WHEN (c.rank = 5 AND n.notecategoryname = 'Rental, Immediate Eviction Notes') THEN n.notetext ELSE NULL END AS rental_eviction_notes
    , CASE WHEN (c.rank = 5 AND n.notecategoryname = 'Rental Campaign, Vulnurable Population Notes') THEN n.notetext ELSE NULL END AS rentcam_vulpop_notes
--Add additional canvass attempts by copying the previous canvass attempt data chunk and incrementing the rank
FROM canvassing_cte c

LEFT JOIN tmc_van.aya_contacts_mym m
	ON m.vanid = c.vanid
LEFT JOIN tmc_van.aya_contactsnotes_mym n
	ON n.vanid||n.createdby||LEFT(n.datecreated,10) = c.concat_field
LEFT JOIN tmc_van.aya_contactsemails_mym e
	ON e.contactsemailid = m.emailid
LEFT JOIN tmc_van.aya_contactsaddresses_mym a
	ON a.contactsaddressid = m.maddressid
LEFT JOIN tmc_van.aya_contactsphones_mym p
	ON p.contactsphoneid = m.phoneid
LEFT JOIN tmc_van.tsm_tmc_phonetypes pt 
	ON pt.phonetypeid = p.phonetypeid

ORDER BY 1 ASC
