import numpy as np
from scipy.spatial.distance import cdist

from sklearn.utils import check_random_state


def initialize_random(x, k, random_state=None, eps=1e-12):
    """Selects initial points randomly from the data.

    Parameters
    ----------
    x : :class:`np.ndarray`
        (n_samples, n_features)
        The original data.
    k : int
        The number of points to select.
    random_state : int or :class:`np.random.RandomState`, optional
        The generator used for initialization. Using an integer fixes the seed.

    Returns
    -------
    Unitialized memberships
    selection : :class:`np.ndarray`
        (k, n_features)
        A length-k subset of the original data.

    """
    n_samples = x.shape[0]
    seeds = check_random_state(random_state).permutation(n_samples)[:k]
    selection = x[seeds] + eps
    distances = cdist(x, selection)
    normalized_distance = distances / np.sum(distances, axis=1)[:, np.newaxis]
    return 1 - normalized_distance, selection


class CMeans:
    """Base class for C-means algorithms.

    Parameters
    ----------
    n_clusters : int, optional
        The number of clusters to find.
    n_init : int, optional
        The number of times to attempt convergence with new initial centroids.
    max_iter : int, optional
        The number of cycles of the alternating optimization routine to run for
        *each* convergence.
    tol : float, optional
        The stopping condition. Convergence is considered to have been reached
        when the objective function changes less than `tol`.
    verbosity : int, optional
        The verbosity of the instance. May be 0, 1, or 2.

        .. note:: Very much not yet implemented.

    random_state : :obj:`int` or :obj:`np.random.RandomState`, optional
        The generator used for initialization. Using an integer fixes the seed.
    eps : float, optional
        To avoid numerical errors, zeros are sometimes replaced with a very
        small number, specified here.

    Attributes
    ----------
    metric : :obj:`string` or :obj:`function`
        The distance metric used. May be any of the strings specified for
        :obj:`cdist`, or a user-specified function.
    initialization : function
        The method used to initialize the cluster centers.
    centers : :obj:`np.ndarray`
        (n_clusters, n_features)
        The derived or supplied cluster centers.
    memberships : :obj:`np.ndarray`
        (n_samples, n_clusters)
        The derived or supplied cluster memberships.

    """

    metric = 'euclidean'
    initialization = staticmethod(initialize_random)

    def __init__(self, n_clusters=2, n_init=10, max_iter=300, tol=1e-4,
                 verbosity=0, random_state=None, eps=1e-18, **kwargs):
        self.n_clusters = n_clusters
        self.n_init = n_init
        self.max_iter = max_iter
        self.tol = tol
        self.verbosity = verbosity
        self.random_state = random_state
        self.eps = eps
        self.params = kwargs
        self.centers = None
        self.memberships = None

    def distances(self, x):
        """Calculates the distance between data x and the centers.

        The distance, by default, is calculated according to `metric`, but this
        method should be overridden by subclasses if required.

        Parameters
        ----------
        x : :obj:`np.ndarray`
            (n_samples, n_features)
            The original data.

        Returns
        -------
        :obj:`np.ndarray`
            (n_samples, n_clusters)
            Each entry (i, j) is the distance between sample i and cluster
            center j.

        """
        return cdist(x, self.centers, metric=self.metric)

    def calculate_memberships(self, x):
        raise NotImplementedError(
            "`calculate_memberships` should be implemented by subclasses.")

    def calculate_centers(self, x):
        raise NotImplementedError(
            "`calculate_centers` should be implemented by subclasses.")

    def objective(self, x):
        raise NotImplementedError(
            "`objective` should be implemented by subclasses.")

    def fit(self, x):
        """Optimizes cluster centers by restarting convergence several times.

        Parameters
        ----------
        x : :obj:`np.ndarray`
            (n_samples, n_features)
            The original data.

        """
        objective_best = np.infty
        memberships_best = None
        centers_best = None
        j_list = []
        for i in range(self.n_init):
            self.centers = None
            self.memberships = None
            self.converge(x)
            objective = self.objective(x)
            j_list.append(objective)
            if objective < objective_best:
                memberships_best = self.memberships.copy()
                centers_best = self.centers.copy()
                objective_best = objective
        self.memberships = memberships_best
        self.centers = centers_best
        return j_list

    def converge(self, x):
        """Finds cluster centers through an alternating optimization routine.

        Terminates when either the number of cycles reaches `max_iter` or the
        objective function changes by less than `tol`.

        Parameters
        ----------
        x : :obj:`np.ndarray`
            (n_samples, n_features)
            The original data.

        """
        centers = []
        j_new = np.infty
        for i in range(self.max_iter):
            j_old = j_new
            self.update(x)
            centers.append(self.centers)
            j_new = self.objective(x)
            if np.abs(j_old - j_new) < self.tol:
                break
        return np.array(centers)

    def update(self, x):
        """Updates cluster memberships and centers in a single cycle.

        If the cluster centers have not already been initialized, they are
        chosen according to `initialization`.

        Parameters
        ----------
        x : :obj:`np.ndarray`
            (n_samples, n_features)
            The original data.

        """
        self.initialize(x)
        self.memberships = self.calculate_memberships(x)
        self.centers = self.calculate_centers(x)

    def initialize(self, x):
        if self.centers is None and self.memberships is None:
            self.memberships, self.centers = \
                self.initialization(x, self.n_clusters, self.random_state)
        elif self.memberships is None:
            self.memberships = \
                self.initialization(x, self.n_clusters, self.random_state)[0]
        elif self.centers is None:
            self.centers = \
                self.initialization(x, self.n_clusters, self.random_state)[1]


class Fuzzy(CMeans):
    """Base class for fuzzy C-means clusters.

    Attributes
    ----------
    m : float
        Fuzziness parameter. Higher values reduce the rate of drop-off from
        full membership to zero membership.

    Methods
    -------
    fuzzifier(memberships)
        Fuzzification operator. By default, for memberships $u$ this is $u^m$.
    objective(x)
        Interpretable as the data's weighted rotational inertia about the
        cluster centers. To be minimised.

    """

    m = 2

    def fuzzifier(self, memberships):
        return np.power(memberships, self.m)

    def objective(self, x):
        if self.memberships is None or self.centers is None:
            return np.infty
        distances = self.distances(x)
        return np.sum(self.fuzzifier(self.memberships) * distances)


class Probabilistic(Fuzzy):
    """Probabilistic C-means.

    In the probabilistic algorithm, sample points have total membership of
    unity, distributed equally among each of the centers. This tends to push
    cluster centers away from each other.

    Methods
    -------
    calculate_memberships(x)
        Memberships are calculated from the distance :math:`d_{ij}` between the
        sample :math:`j` and the cluster center :math:`i`.

        .. math::

            u_{ik} = \left(\sum_j \left( \\frac{d_{ik}}{d_{jk}} \\right)^{\\frac{2}{m - 1}} \\right)^{-1}

    calculate_centers(x)
        New centers are calculated as the mean of the points closest to them,
        weighted by the fuzzified memberships.

        .. math:: c_i = \left. \sum_k u_{ik}^m x_k \middle/ \sum_k u_{ik} \\right.

    """

    def calculate_memberships(self, x):
        distances = self.distances(x)
        distances[distances == 0.] = 1e-18
        return np.sum(np.power(
            np.divide(distances[:, :, np.newaxis], distances[:, np.newaxis, :]),
            2 / (self.m - 1)), axis=2) ** -1

    def calculate_centers(self, x):
        return np.dot(self.fuzzifier(self.memberships).T, x) / \
               np.sum(self.fuzzifier(self.memberships).T, axis=1)[..., np.newaxis]
