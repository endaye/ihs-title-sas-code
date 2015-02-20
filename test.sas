option compress = yes;

libname f "/opt/data/datamain/TITLE2012/sas_dataset";
libname h "/home/yzhang96/TITLE";

%macro test;
filename filelist pipe "ls /opt/data/datamain/TITLE2012/sas_dataset";
data h.f_list;
   infile filelist;
   length process $ 80;
   input process $ char80.;
   call symput('f_num', _N_);
run;
proc print data=h.f_list;
run;

proc contents data = f.dpl_il_cook_20120507 out = h.a position noprint;
run;

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
if last.maxlength then output;
run;

%mend test;

%test();

