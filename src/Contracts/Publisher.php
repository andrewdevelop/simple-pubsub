<?php 

namespace Core\Messaging\Contracts;

interface Publisher
{
    /**
     * @param string $str_message
     * @return mixed
     */
	public function publish(string $str_message);
}