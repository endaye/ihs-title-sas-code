option compress =yes;
%LET txtds = ./;
libname f './';

%macro import_single(filein, fileout=, default_over=truncover, default_dlm='7C'x, default_maxlen=6000);
*** set default output dataset name ***;
%if &fileout= %then %do;
	%let fileout = %sysfunc(cats(sasds., %scan(%scan(&filein.,-1,/),1,.)));
%end;


*** read nobs from Batch Head ***;
data ds_0;
infile "&filein." dsd &default_over. lrecl=99999999 dlm=&default_dlm. obs=1;
input level: $1. Doc_Identifier: $1. FIPS: $5. Batch_Date: $8. Rec_Count: $9. Time_From: $23. Time_To: $23.;
call symputx("obsnum", rec_count*1-1);
call symput('dt_batch', Batch_Date);
run;
%put ~~~ Obs Number from Batch Header: &obsnum ~~~;


*** clean dirty value that splits into multiple lines ***;
data ds_1;
infile "&filein." &default_over. lrecl=99999999 firstobs=2;
input raw_all $&default_maxlen..;
*** 四个变量: raw_all原始数据（默认最大长度6000）, obs_tmp编号, level_tmp种类, flag_tmp标记成一块一块的;
raw_all = compress(raw_all,&default_dlm.,'kw');
obs_tmp = _N_;
level_tmp = substr(raw_all,1,1);
if (level_tmp='H' | level_tmp='R' | level_tmp='I' | level_tmp='M' | level_tmp='P' | level_tmp='A' 
	| level_tmp='L' | level_tmp='N' | level_tmp='T') & substr(raw_all,2,1)=&default_dlm. then flag_tmp=0;
else flag_tmp++1;
run;

data f.ds_1;
set ds_1;
run;
*** Variable, max,;
proc means data=ds_1 noprint; var flag_tmp; output out=ds_tmp max=max; run;
data f.ds_tmp; set ds_tmp; run;
data _NULL_;
set ds_tmp;
call symputx('max_flag',max);
run;
%put ~~~ max_flag = &max_flag ~~~;

*** 如果有脏数据存在;
%if &max_flag. >= 1 %then %do;
	%do i = 0 %to &max_flag.;
		data ds_tmp&i.;
		set ds_1(where=(flag_tmp=&i.));
		id_tmp = obs_tmp - flag_tmp;
		keep raw_all id_tmp;
		rename raw_all = raw_&i.;
		run;

		data f.ds_tmp&i.; set ds_tmp&i.; run;
	%end;
	
	data ds_2;
	merge ds_tmp0 - ds_tmp&max_flag.;
	by id_tmp;
	format raw_all $&default_maxlen..;
	raw_all = cats(OF raw_0 - raw_&max_flag.);
	keep raw_all;
	run;
%end;
%else %do;
	data ds_2; set ds_1; keep raw_all; run;
%end;


*** read data with cleaned string format from dictionary ***;
data ds_3;
set ds_2;
%do i = 1 %to &maxvar.;
	format &&tmpvar&i &&fmt&i;
%end;
format level $1.;
level = scan(raw_all,1,&default_dlm.,'m');
if level='H' then do; %do i = 1 %to &maxh.; hvar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
else if level='R' then do; %do i = 1 %to &maxr.; rvar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
else if level='I' then do; %do i = 1 %to &maxi.; ivar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
else if level='M' then do; %do i = 1 %to &maxm.; mvar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
else if level='P' then do; %do i = 1 %to &maxp.; pvar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
else if level='A' then do; %do i = 1 %to &maxa.; avar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
else if level='L' then do; %do i = 1 %to &maxl.; lvar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
else if level='N' then do; %do i = 1 %to &maxn.; nvar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
else if level='T' then do; %do i = 1 %to &maxt.; tvar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
else do; put 'ERROR: Unknown Level Type ' level 'in Obs: ' _N_; end;
if level^='I' & level^='M' then indi_idx=0;
else if level='I' then indi_idx++1;
else indi_idx++0;
%do i = 1 %to &maxvar.;
	rename &&tmpvar&i = &&var&i;
%end;
call symputx("maxobs", _N_);
drop raw_all;
run;
%put ~~~ Max Obs Read from File: &maxobs ~~~;

*** compare nobs with Batch Head ***;
%if &obsnum^=&maxobs %then %do;
	%put "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~";
	%put "ERROR: Observations Read from TXT File NOT Equal to Number Specified in Batch Header";
	%put "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~";
%end;


*** split data into different level types sub datasets ***;
data ds_h(keep=H_:) ds_r(keep=R_:) ds_i(keep=indi_idx I_:) ds_m(keep=indi_idx M_:)
	ds_p(keep=P_:) ds_a(keep=A_:) ds_l(keep=L_:) ds_n(keep=N_:) ds_t(keep=T_:);
set ds_3;
if level='H' then output ds_h;
else if level='R' then output ds_r;
else if level='I' then output ds_i;
else if level='M' then output ds_m;
else if level='P' then output ds_p;
else if level='A' then output ds_a;
else if level='L' then output ds_l;
else if level='N' then output ds_n;
else if level='T' then output ds_t;
else do; 
	put 'ERROR: Unknown Level Type ' level 'in Obs: ' _N_; 
end;
run;


*** combine I & M level ***;
proc sort data=ds_i nodupkey; by I_Doc_Identifier indi_idx; run;
proc sort data=ds_m nodupkey; by M_Doc_Identifier indi_idx; run;
data ds_im;
merge ds_i(in=a rename=(I_Doc_Identifier=Doc_Identifier)) ds_m(in=b rename=(M_Doc_Identifier=Doc_Identifier));
by Doc_Identifier indi_idx;
run;

%mend import_single;
%import_single(&txtds.DPL_IL_Cook_20150302.txt);