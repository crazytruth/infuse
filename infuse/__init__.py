import pkg_resources

__version__ = pkg_resources.get_distribution('infuse').version

from infuse.app import Infuse

__all__ = ['__version__', 'Infuse']
