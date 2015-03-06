option compress = yes;

libname f "/opt/data/datamain/TITLE2012/sas_dataset";
libname h "/home/yzhang96/TITLE";

%macro test_all();
	filename filelist pipe "ls /opt/data/datamain/TITLE2012/sas_dataset";
	data h.f_list;
		infile filelist;
		length file $ 80;
		input file $ char80.;
		name = scan(file, 1, ".");
		call symput("f_all", _N_);
		call symput(cat("var", _N_), _N_);
		call symput(cats("f_name", _N_), name);
	run;
	proc print data = h.f_list;
	run;

	%put ~~~~~~~~~~;
	%do i = 1 %to 2;
		%put ~~~~~file No. &&var&i. ~~~~file name: &&f_name&i.;
		%test(&&f_name&i.);
	%end;

%mend test_all;

%macro test(f_name);
	proc contents data = f.&f_name. out = a_&f_name. position noprint;
	run;

	data h.b_&f_name.;
		set a_&f_name.;
		keep MEMNAME NAME TYPE LENGTH VARNUM FORMAT;
	run;
	proc sort data = h.b_&f_name.;
	by VARNUM; 
	run;

	data h.&f_name._var_list;
		set h.b_&f_name;
		call symput("var_all", _N_);
		call symput(cat("v", _N_), _N_);
		call symput(cat("vname", _N_), name);
		call symput(cat("vlen", _N_), length);
	run;

	data h.&f_name.2;
		set h.&f_name.;
		%do i = 1 %to &var_all.;
			
	/*
	data h.x;
		set f.dpl_il_cook_20120507;
		retain maxlength 0;
		a1_apn_pin1 = trim(A1_APN_PIN);
		len = length(A1_APN_PIN);
		if maxlength<len then maxlength=len;
	run;

	data h.y;
		set h.x;
		by a1_apn_pin;
		if maxlength then output;
	run;
	*/
%mend test;

%test_all();
*%test();

