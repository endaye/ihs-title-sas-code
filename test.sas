option compress = yes;

libname f "/opt/data/datamain/TITLE2012/sas_dataset";
libname h "/home/yzhang96/TITLE";

%macro test_all();
filename filelist pipe "ls /opt/data/datamain/TITLE2012/sas_dataset";
data h.f_list;
	infile filelist;
	length file $ 80;
	input file $ char80.;
	name = scan(file,1,'.sas7bdat');
	call symput('f_all', _N_);
	call symput(cat('var', _N_), _N_);
	call symput(cats('f_name',_N_), name);
run;
proc print data = h.f_list;
run;

%put ~~~~~~~~~~;

%do i=1 %to &f_all.;
	%put ~~~~~file No. &&var&i. ~~~~file name: &&f_name&i.;
	%test(&&f_name&i.);
%end;

%mend test_all;

%macro test(&f_name);
proc contents data = f.&f_name. out = h.a_&f_name. position noprint;
run;
/*
data h.b;
set h.a;
keep MEMNAME NAME TYPE LENGTH VARNUM FORMAT;
run;
proc sort data = h.b;
by VARNUM; 
run;

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

