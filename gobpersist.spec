%define prefix /opt/accellion/lib/python2.7/site-packages
%define name gobpersist
%define version 0.1
%define release 1

Summary: Gobpersist Package
Name: %{name}
Version: %{version}
Release: %{release}
Vendor: Accellion Inc
Group: Development/Libraries
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
URL: http://www.accellion.com
License: All rights reserved, Accellion Pte Ltd
BuildArch: noarch

%description
Gobpersist Library

%prep
%setup -q -n %{name}-%{version}

%build

%install
[ "$RPM_BUILD_ROOT" != "/" ] && rm -rf $RPM_BUILD_ROOT
mkdir -m 755 -p $RPM_BUILD_ROOT%{prefix}
cp -a * $RPM_BUILD_ROOT%{prefix}

%clean
[ "$RPM_BUILD_ROOT" != "/" ] && rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%{prefix}
