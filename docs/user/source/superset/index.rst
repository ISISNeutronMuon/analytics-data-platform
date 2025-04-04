.. _superset:

========
Superset
========

.. toctree::
   :glob:
   :hidden:

   *

Quick links
-----------

- `ISIS Superset <isis-superset_>`_

Getting started
---------------

`Apache Superset <apache-superset_>`_ is a data exploration and visualization tool
allowing users to explore their data by create charts and dashboards without writing code.
It is an off-the-shelf, open-source tool used extensively by the analytics community
and is not a custom tool built for ISIS.

ISIS hosts its own `instance of this tool <isis-superset_>`_ with the available
datasets preconfigured.

Logging in
^^^^^^^^^^

When visiting `Superset <isis-superset_>`_ for the first time you will
be presented with a login screen:

.. image:: _images/isis-superset-login-screen.png
   :align: center
   :scale: 75%

| Use your STFC email address and password to login.

Home page
^^^^^^^^^

After successfully logging in you are redirected to the home page. This page
displays a selection of the dashboards & charts that have been accessed recently,
providing a way to quickly jump to content that is most frequently accessed.
*Note: The home page is customized to each user so will look different depending*
*who is logged in. The pixelated parts of the image will look different when you*
*view the page as you.*

.. image:: _images/isis-superset-example-home-page.png
   :align: center
   :scale: 40%

|

The header within the *Charts* & *Dashboards* sections contains the following
buttons:

- **Favorite**: Lists all items that have been favorited using the *star* buttons.
- **Mine**: Lists all items created by the logged in user.
- **All**: Lists the 5 most recently accessed items.

To view all items of that type within the system click the ``View All >>`` link
on the right-hand side of the section header.

Charts
^^^^^^

Charts are one of the central features of Superset.
Please see the :ref:`superset-charts` documentation for more detail.

.. include:: ../common_links.rst
