%global module_api %(qore --latest-module-api 2>/dev/null)
%{?_datarootdir: %global mydatarootdir %_datarootdir}
%{!?_datarootdir: %global mydatarootdir %{buildroot}/usr/share}

%global module_dir %{_libdir}/qore-modules
%global user_module_dir %{mydatarootdir}/qore-modules/

Name:           qore-odbc-module
Version:        1.2.0
Release:        1
Summary:        Qorus Integration Engine - Qore odbc module
License:        MIT
Group:          Development/Languages/Other
Url:            https://qoretechnologies.com
Source:         qore-odbc-module-%{version}.tar.bz2
BuildRequires:  gcc-c++
%if 0%{?el7}
BuildRequires:  devtoolset-7-gcc-c++
%endif
BuildRequires:  cmake >= 3.5
BuildRequires:  qore-devel >= 1.14
BuildRequires:  qore-stdlib >= 1.14
BuildRequires:  qore >= 1.14
BuildRequires:  doxygen
BuildRequires:  unixODBC-devel
Requires:       unixODBC
Requires:       %{_bindir}/env
Requires:       qore >= 1.0
BuildRoot:      %{_tmppath}/%{name}-%{version}-build

%description
This package contains the odbc module for the Qore Programming Language.

%prep
%setup -q

%build
%if 0%{?el7}
# enable devtoolset7
. /opt/rh/devtoolset-7/enable
unset msgpackPATH
%endif
export CXXFLAGS="%{?optflags}"
cmake -DCMAKE_INSTALL_PREFIX=%{_prefix} -DCMAKE_BUILD_TYPE=RELWITHDEBINFO -DCMAKE_SKIP_RPATH=1 -DCMAKE_SKIP_INSTALL_RPATH=1 -DCMAKE_SKIP_BUILD_RPATH=1 -DCMAKE_PREFIX_PATH=${_prefix}/lib64/cmake/Qore .
make %{?_smp_mflags}
make %{?_smp_mflags} docs
sed -i 's/#!\/usr\/bin\/env qore/#!\/usr\/bin\/qore/' test/*.qtest

%install
make DESTDIR=%{buildroot} install %{?_smp_mflags}

%files
%{module_dir}

%check
qore -l ./odbc-api-%{module_api}.qmod test/odbc.qtest

%package doc
Summary: Documentation and examples for the Qore odbc module
Group: Development/Languages

%description doc
This package contains the HTML documentation and example programs for the Qore
odbc module.

%files doc
%defattr(-,root,root,-)
%doc docs/odbc test

%changelog
* Sat Jan 28 2023 David Nichols <david@qore.org>
- 1.2.0 release

* Mon Jan 9 2023 David Nichols <david@qore.org>
- 1.1.3 release

* Mon Dec 19 2022 David Nichols <david@qore.org>
- 1.1.2 release

* Wed Oct 26 2022 David Nichols <david@qore.org>
- initial 1.1.1 release
